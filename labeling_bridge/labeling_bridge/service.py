"""API service module."""
from datetime import datetime
import json
import logging
import os
from pathlib import Path
import tempfile

import fiftyone as fo
import fiftyone.server.view as fosv
from fiftyone import ViewField as F
from base.aws.container_services import ContainerServices
from base.aws.s3 import S3Controller
from base.voxel import functions as voxel_functions
from base.voxel.constants import GT_POSE_LABEL, GT_SEMSEG_LABEL, OPENLABEL_LABEL_MAPPING
from labeling_bridge.kognic_interface import KognicInterface
from labeling_bridge.models.api import (
    RequestExportJobDTO,
    RequestExportMethodDTO,
    RequestImportJobDTO,
    KognicLabelingTypeDTO
)
from labeling_bridge.models.database import StatusDocument, Status
from labeling_bridge.repository import Repository
from labeling_bridge.models.database.enums import KognicLabelingType

BATCH_SIZE = 16
KOGNIC_IMPORT_TAG = "Labeled"

_logger = logging.getLogger("labeling_bridge." + __name__)

fields_to_delete = ["OpenLABEL_id", "interpolated", "is_hole",
                    "mode", "name", "stream", "order"]

annotation_type_field = {"2D_semseg": f"{GT_SEMSEG_LABEL}.detections.",
                         "Splines": f"{GT_POSE_LABEL}.keypoints."}


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

    def kognic_import(self, request_import_job_dto: RequestImportJobDTO):  # pylint: disable=too-many-locals,too-many-statements,too-many-branches
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

        openlabel_skeleton = voxel_functions.openlabel_skeleton()
        dataset = fo.load_dataset(request_import_job_dto.dataset_name)
        _logger.debug("Using dataset : %s", dataset)

        for annotation_type in batch_annotation_types:
            with tempfile.TemporaryDirectory() as tmp_dir_name:
                # Set up a filepath map to keep image source after the merge
                fn_map = {}
                # Download annotations to disk
                try:
                    _logger.debug("Downloading and processing annotations of type: %s", annotation_type.value)
                    all_batch_annotations = kognic_client.get_project_annotations(
                        request_import_job_dto.kognic_project_id,
                        request_import_job_dto.labelling_job_name,
                        annotation_type)
                    for annotation in all_batch_annotations:
                        if annotation_type == KognicLabelingTypeDTO.BODYPOSE:
                            raise NotImplementedError()
                        external_id = annotation.content["openlabel"]["frames"]["0"]["frame_properties"]["external_id"]

                        # checks if labels from the labeling job already exists on the sample
                        if label_from_job_exists(external_id, request_import_job_dto.labelling_job_name,
                                                 annotation_type, dataset):
                            _logger.debug("%s labels from labeling_job %s already exists on sample %s",
                                          annotation_type, request_import_job_dto.labelling_job_name, external_id)
                        else:
                            if annotation.content["openlabel"]["frames"]["0"].get("objects") is not None:
                                for _, an_object in annotation.content["openlabel"]["frames"]["0"]["objects"].items():
                                    an_object["labeling_job"] = request_import_job_dto.labelling_job_name

                                a_external_id = os.path.basename(external_id)
                            json_annotation = json.dumps(annotation.content, indent=4)
                            _logger.debug("File content:\n%s", str(json_annotation))
                            filename = tmp_dir_name + "/" + a_external_id + ".json"
                            with open(filename, "w", encoding="utf-8") as annotation_file:
                                annotation_file.write(str(json_annotation))
                                annotation_file.close()
                                _logger.debug("Annotation for id %s with filename %s downloaded",
                                              a_external_id, filename)

                                fn_map[a_external_id] = external_id
                except Exception as err:  # pylint: disable=broad-except
                    msg = f"""Failed to get completed annotations on project {request_import_job_dto.kognic_project_id},
                                batch {request_import_job_dto.labelling_job_name},
                                annotation type {annotation_type.value}."""
                    _logger.exception("%s\nException: %s", str(msg), err)
                    Repository.update_status_of_tasks(
                        labeling_jobs=labeling_jobs,
                        kognic_labeling_types=[KognicLabelingType[annotation_type.name]],
                        status=StatusDocument(
                            status=Status.ERROR,
                            message=msg))
                    return
                _logger.debug(
                    "Annotation files of type %s downloaded: %s",
                    annotation_type.value,
                    os.listdir(tmp_dir_name))

                if dataset.skeletons.get(GT_POSE_LABEL) is None:
                    _logger.debug("Body pose skeletons not found, adding them")
                    voxel_functions.set_dataset_skeleton_configuration(dataset)

                try:
                    dataset.merge_dir(dataset_type=fo.types.OpenLABELImageDataset,
                                      merge_lists=True,
                                      overwrite=False,
                                      data_path=fn_map,
                                      labels_path=tmp_dir_name,
                                      dynamic=True,
                                      insert_new=False,
                                      tags=KOGNIC_IMPORT_TAG,
                                      skeleton=openlabel_skeleton,
                                      skeleton_key="point_class",
                                      label_field=OPENLABEL_LABEL_MAPPING
                                      )
                    print(fn_map)

                    for field in fields_to_delete:
                        dataset.delete_sample_field(annotation_type_field[annotation_type] + field)
                    dataset.save()
                    #
                    # Get ids of labels where we need to set the date
                    label_ids = dataset.filter_labels(
                        annotation_type_field[annotation_type].split(".")[0],
                        (~F("date").exists()) & (F("labeling_job") == request_import_job_dto.labelling_job_name)
                    ).values(f"{annotation_type_field[annotation_type]}id", unwind=True)
                    # Map the date to the id
                    today = datetime.combine(datetime.today(), datetime.min.time())
                    values = {_id: today for _id in label_ids}
                    # Set the label_values
                    dataset.set_label_values(f"{annotation_type_field[annotation_type]}date", values)

                    # Add all new fields
                    dataset.add_dynamic_sample_fields()

                    dataset.save()
                    #
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


def label_from_job_exists(filepath: str, labeling_job: str, annotation_type: str, dataset):
    """
    Verifies wheter the sample with the given filepath
    already has labels with the respective annotation_type from the given labeling_job

    Args:
        filepath (str): sample filepath
        labeling_job (str): kognic batch
        annotation_type (str): label annotation type
        dataset (fo.Dataset): voxel dataset

    Returns:
        Boolean: True if has labels, False if not
    """
    sample = dataset[filepath]
    label_fields_split = annotation_type_field[annotation_type].split(".")
    if sample.has_field(label_fields_split[0]) and \
            (label_type := sample.get_field(label_fields_split[0])) is not None:
        if label_type.has_field(label_fields_split[1]) and \
                (labels := label_type.get_field(label_fields_split[1])) is not None:
            for label in labels:
                if label.has_field("labeling_job") and label.get_field("labeling_job") == labeling_job:
                    return True

    return False
