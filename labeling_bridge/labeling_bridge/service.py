"""API service module."""
from datetime import datetime
import json
import logging
import os
from pathlib import Path
import tempfile

import fiftyone as fo
import fiftyone.server.view as fosv
from base.aws.container_services import ContainerServices
from base.aws.s3 import S3Controller
from labeling_bridge.kognic_interface import KognicInterface
from labeling_bridge.models.api import RequestExportJobDTO, RequestExportMethodDTO, RequestImportJobDTO
from labeling_bridge.models.database import StatusDocument, Status
from labeling_bridge.repository import Repository
from labeling_bridge.models.database.enums import KognicLabelingType

BATCH_SIZE = 16
KOGNIC_IMPORT_TAG = "Labeled"

_logger = logging.getLogger("labeling_bridge." + __name__)


class ApiService:  # pylint: disable=too-many-instance-attributes,too-few-public-methods
    """API service class."""

    def __init__(self, s3_client, container_services: ContainerServices, kognic_interface_factory=KognicInterface):
        self.__s3 = s3_client
        self.__container_services: ContainerServices = container_services
        self.kognic_interface_factory = kognic_interface_factory

    def kognic_export(self, request_export_job_dto: RequestExportJobDTO):  # pylint: disable=too-many-locals
        """
        Args:
            request_data (Dict): dict with information on what to export to kognic
        Raises:

        """
        # create kognic interface instance
        kognic_client = self.kognic_interface_factory(
            request_export_job_dto.client_id,
            request_export_job_dto.client_secret)

        # create batch if doesn't exist
        if not kognic_client.verify_batch(
                request_export_job_dto.kognic_project_id,
                request_export_job_dto.labelling_job_name):
            kognic_client.create_batch(
                request_export_job_dto.kognic_project_id,
                request_export_job_dto.labelling_job_name)
            _logger.info("Batch created: %s on project %s",
                         request_export_job_dto.labelling_job_name,
                         request_export_job_dto.kognic_project_id)
        # We need to verify if we are importing another dataset to the same batch
        labeling_jobs = Repository.get_labeling_jobs(
            kognic_project_id=request_export_job_dto.kognic_project_id,
            kognic_labeling_job_name=request_export_job_dto.labelling_job_name)
        for labeling_job in labeling_jobs:
            if labeling_job.voxel_dataset != request_export_job_dto.dataset_name:
                raise ValueError("You are not allowed to export a different voxel dataset to the same kognic batch.")

        voxel_dataset = fo.load_dataset(request_export_job_dto.dataset_name)

        if request_export_job_dto.voxel_export_method == RequestExportMethodDTO.TAG:
            filtered_dataset = voxel_dataset.match_tags(request_export_job_dto.tag)
        elif request_export_job_dto.voxel_export_method == RequestExportMethodDTO.FILTER:
            filtered_dataset = fosv.get_view(
                request_export_job_dto.dataset_name,
                stages=request_export_job_dto.stages,
                filters=request_export_job_dto.filters,
                count_label_tags=True,
            )
        else:
            raise NotImplementedError(f"Unknown export method: {request_export_job_dto.voxel_export_method}")

        # Store information of samples
        labeling_job = Repository.generate_job_and_task_entries(
            dataset_view=filtered_dataset,
            user_email=kognic_client.user_email,
            request_export_job_dto=request_export_job_dto)

        dataset_size = filtered_dataset.count()
        for batch in range(0, dataset_size, BATCH_SIZE):
            dataset_view = filtered_dataset[batch:batch + BATCH_SIZE]
            file_paths, raw_file_paths = dataset_view.values(["filepath", "raw_filepath"])
            for file_path, raw_file_path in zip(file_paths, raw_file_paths):
                with tempfile.NamedTemporaryFile(suffix=Path(raw_file_path).suffix) as temp_file:
                    # download
                    bucket, file_name = S3Controller.get_s3_path_parts(raw_file_path)
                    self.__container_services.download_file_to_disk(self.__s3, bucket, file_name, temp_file.name)
                    try:
                        kognic_client.upload_image(
                            request_export_job_dto.kognic_project_id,
                            request_export_job_dto.labelling_job_name,
                            # this method accepts a array of possible exports
                            [val.value for val in request_export_job_dto.labelling_types],
                            file_path,
                            temp_file.name)
                        # Updating task status
                        Repository.update_status_of_tasks(
                            labeling_jobs=[labeling_job],
                            kognic_labeling_types=[KognicLabelingType[val.name]
                                                   for val in request_export_job_dto.labelling_types],
                            status=StatusDocument(
                                status=Status.PROCESSING),
                            extra_set={"exported_to_kognic_at": datetime.utcnow()},
                            extra_filters={"raw_media_filepath": raw_file_path})
                    except Exception as exp:  # pylint: disable=broad-exception-caught
                        _logger.exception("Failed to export: %s %s \nException: %s", bucket, file_name, str(exp))
                        Repository.update_status_of_tasks(
                            labeling_jobs=[labeling_job],
                            kognic_labeling_types=[KognicLabelingType[val.name]
                                                   for val in request_export_job_dto.labelling_types],
                            status=StatusDocument(
                                status=Status.ERROR,
                                message=f"Error: {str(exp)}"),
                            extra_filters={"raw_media_filepath": raw_file_path})

        Repository.update_status_of_jobs([labeling_job], StatusDocument(status=Status.PROCESSING))

    def kognic_import(self, request_import_job_dto: RequestImportJobDTO):  # pylint: disable=too-many-locals
        """
        Import label metadata from Kognic into Voxel
        Args:
            request_data: Dict with data about what to import from Kognic

        """
        _logger.debug("Importing labeling job from Kognic into Voxel.")

        # create kognic interface instance
        kognic_client = self.kognic_interface_factory(
            request_import_job_dto.client_id,
            request_import_job_dto.client_secret)

        batch_annotation_types = kognic_client.get_annotation_types(
            request_import_job_dto.kognic_project_id, request_import_job_dto.labelling_job_name)

        labeling_jobs = Repository.get_labeling_jobs(
            kognic_project_id=request_import_job_dto.kognic_project_id,
            kognic_labeling_job_name=request_import_job_dto.labelling_job_name)
        for labeling_job in labeling_jobs:
            if labeling_job.voxel_dataset != request_import_job_dto.dataset_name:
                raise ValueError("You are not allowed to import a kognic batch into a different Vvoxel dataset.")

        for annotation_type in batch_annotation_types:
            with tempfile.TemporaryDirectory() as tmp_dir_name:
                # Download annotations to disk
                try:
                    _logger.debug("Downloading and processing annotations of type: %s", annotation_type.value)
                    all_batch_annotations = kognic_client.get_project_annotations(
                        request_import_job_dto.kognic_project_id,
                        request_import_job_dto.labelling_job_name,
                        annotation_type)
                    for annotation in all_batch_annotations:
                        a_external_id = os.path.basename(
                            annotation.content["openlabel"]["frames"]["0"]["frame_properties"]["external_id"])
                        json_annotation = json.dumps(annotation.content, indent=4)
                        _logger.debug("File content:\n%s", str(json_annotation))
                        filename = tmp_dir_name + "/" + a_external_id + ".json"
                        with open(filename, "w", encoding="utf-8") as annotation_file:
                            annotation_file.write(str(json_annotation))
                            annotation_file.close()
                            _logger.debug("Annotation for id %s with filename %s downloaded", a_external_id, filename)
                except Exception as err:  # pylint: disable=broad-except
                    msg = f"""Failed to get completed annotations on project {request_import_job_dto.kognic_project_id},
                                batch {request_import_job_dto.labelling_job_name},
                                annotation type {annotation_type.value}."""
                    _logger.exception(msg, "\nException: %s", err)
                    Repository.update_status_of_tasks(
                        labeling_jobs=labeling_jobs,
                        kognic_labeling_types=[KognicLabelingType[annotation_type.name]],
                        status=StatusDocument(
                            status=Status.ERROR,
                            message=msg))

                _logger.debug(
                    "Annotation files of type %s downloaded: %s",
                    annotation_type.value,
                    os.listdir(tmp_dir_name))

                # Set the Dataset where we will be importing to
                dataset = fo.load_dataset(request_import_job_dto.dataset_name)
                _logger.debug("dataset : %s", dataset)

                # Setup a filepath map to keep image source after the merge
                filepaths = dataset.values("filepath")
                fn_map = {os.path.basename(fp): fp for fp in filepaths}

                # Import all labels from Kognic
                try:
                    dataset.merge_dir(dataset_type=fo.types.OpenLABELImageDataset,
                                      merge_lists=False,
                                      data_path=fn_map,
                                      labels_path=tmp_dir_name,
                                      dynamic=True,
                                      tags=KOGNIC_IMPORT_TAG)
                    dataset.save()
                    _logger.debug("Annotation files of type %s merged!", annotation_type.value)
                    Repository.update_status_of_tasks(
                        labeling_jobs=labeling_jobs,
                        kognic_labeling_types=[KognicLabelingType[annotation_type.name]],
                        status=StatusDocument(
                            status=Status.DONE))
                except Exception as err:  # pylint: disable=broad-except
                    msg = f"Unable to load label files of type {annotation_type.value} from: {tmp_dir_name}"
                    _logger.exception(msg, "\nException: %s", str(err))
                    Repository.update_status_of_tasks(
                        labeling_jobs=labeling_jobs,
                        kognic_labeling_types=[KognicLabelingType[annotation_type.name]],
                        status=StatusDocument(
                            status=Status.ERROR,
                            message=msg))

        Repository.update_status_of_jobs(labeling_jobs, StatusDocument(status=Status.DONE))
