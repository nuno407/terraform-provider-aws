"""API service module."""
import json
import logging
import os
import re
from pathlib import Path
import tempfile
from typing import Dict, Tuple

import fiftyone as fo
import fiftyone.server.view as fosv
from base.aws.container_services import ContainerServices
from labeling_bridge.kognic_interface import KognicInterface

BATCH_SIZE = 16
KOGNIC_IMPORT_TAG = "Labeled"

_logger = logging.getLogger("labeling_bridge." + __name__)


class ApiService:  # pylint: disable=too-many-instance-attributes,too-few-public-methods
    """API service class."""

    def __init__(self, s3_client, container_services: ContainerServices, kognic_interface_factory=KognicInterface):
        self.__s3 = s3_client
        self.__container_services: ContainerServices = container_services
        self.dataset_name = None
        self.kognic_project_id = None
        self.labelling_type = None
        self.labelling_job_name = None
        self.labelling_guidelines = None
        self.export_method = None
        self.client_id = None
        self.client_secret = None
        self.tag = None
        self.filters = None
        self.stages = None
        self.kognic_interface_factory = kognic_interface_factory

    def kognic_export(self, request_data: Dict):  # pylint: disable=too-many-locals
        """
        Args:
            request_data (Dict): dict with information on what to export to kognic
        Raises:

        """
        # parse request_data
        self._parse_request(request_data=request_data)

        # create kognic interface instance
        kognic_client = self.kognic_interface_factory(self.client_id, self.client_secret)

        # create batch if doesn't exist
        if not kognic_client.verify_batch(self.kognic_project_id, self.labelling_job_name):
            kognic_client.create_batch(self.kognic_project_id, self.labelling_job_name)
            _logger.info("Batch created: %s on project %s", self.labelling_job_name, self.kognic_project_id)

        voxel_dataset = fo.load_dataset(self.dataset_name)

        if self.export_method == "tag":
            filtered_dataset = voxel_dataset.match_tags(self.tag)
        else:
            filtered_dataset = fosv.get_view(
                self.dataset_name,
                stages=self.stages,
                filters=self.filters,
                count_label_tags=True,
            )

        dataset_size = filtered_dataset.count()
        for batch in range(0, dataset_size, BATCH_SIZE):
            dataset_view = filtered_dataset[batch:batch + BATCH_SIZE]
            file_paths, raw_file_paths = dataset_view.values(["filepath", "raw_filepath"])
            for file_path, raw_file_path in zip(file_paths, raw_file_paths):
                extension = Path(raw_file_path).suffix
                with tempfile.NamedTemporaryFile(suffix=extension) as temp_file:
                    # download
                    bucket, file_name = self._get_s3_path_parts(raw_file_path)
                    self.__container_services.download_file_to_disk(self.__s3, bucket, file_name, temp_file.name)
                    kognic_client.upload_image(
                        self.kognic_project_id,
                        self.labelling_job_name,
                        self.labelling_type,
                        file_path,
                        temp_file.name)

    def kognic_import(self, request_data: Dict):  # pylint: disable=too-many-locals
        """
        Import label metadata from Kognic into Voxel
        Args:
            request_data: Dict with data about what to import from Kognic

        """
        self._parse_request(request_data)
        _logger.debug("Importing labeling job from Kognic into Voxel.")

        # create kognic interface instance
        kognic_client = self.kognic_interface_factory(self.client_id, self.client_secret)

        batch_annotation_types = kognic_client.get_annotation_types(self.kognic_project_id, self.labelling_job_name)

        for annotation_type in batch_annotation_types:
            with tempfile.TemporaryDirectory() as tmp_dir_name:
                # Download annotations to disk
                try:
                    _logger.debug("Downloading and processing annotations of type: %s", annotation_type)
                    all_batch_annotations = kognic_client.get_project_annotations(self.kognic_project_id,
                                                                                  self.labelling_job_name,
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
                    _logger.exception("Failed to get completed annotations on project %s, batch %s, annotation type %s."
                                      "\nException: %s", self.kognic_project_id, self.labelling_job_name,
                                      annotation_type, err)

                _logger.debug("Annotation files of type %s downloaded: %s", annotation_type, os.listdir(tmp_dir_name))

                # Set the Dataset where we will be importing to
                dataset = fo.load_dataset(self.dataset_name)
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
                    _logger.debug("Annotation files of type %s merged!", annotation_type)
                except Exception:  # pylint: disable=broad-except
                    _logger.exception("Unable to load label files of type %s from: %s", annotation_type, tmp_dir_name)

    def _get_s3_path_parts(self, raw_path) -> Tuple[str, str]:

        match = re.match(r"^s3://([^/]+)/(.*)$", raw_path)

        if match is None or len(match.groups()) != 2:
            raise ValueError("Invalid path: " + raw_path)

        bucket = match.group(1)
        key = match.group(2)
        return bucket, key

    def _parse_request(self, request_data: Dict):
        """ Parse request_data. """
        self.dataset_name = request_data.get("dataset", None)
        self.kognic_project_id = request_data.get("kognicProjectId", None)
        self.labelling_type = request_data.get("labellingType", None)
        self.labelling_job_name = request_data.get("labellingJobName", None)
        self.labelling_guidelines = request_data.get("labellingGuidelines", None)
        self.export_method = request_data.get("voxelExportMethod", None)
        self.client_id = request_data.get("clientId", None)
        self.client_secret = request_data.get("clientSecret", None)
        self.tag = request_data.get("voxelTagToExport", None)
        self.filters = request_data.get("filters", None)
        self.stages = request_data.get("stages", None)
