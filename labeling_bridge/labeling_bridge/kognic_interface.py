"""Kognic Interface Module"""

import logging

import kognic.io.model.input as InputModel
import kognic.io.model.input.cameras as CamerasModel
from kognic.io.client import KognicIOClient

_logger = logging.getLogger("labeling_bridge." + __name__)


class KognicInterface:
    """Kognic Interface Class"""

    def __init__(self, client_id: str, client_secret: str) -> None:
        self.kognic_client = KognicIOClient(auth=(client_id, client_secret))

    def get_annotation_types(self, kognic_project_id: str, batch: str) -> list:
        """Gets annotation types for a given batch name
        Args:
            kognic_project_id : Kognic Project Id

        Returns:
            list : Kognic annotation types
        """
        return self.kognic_client.project.get_annotation_types(project=kognic_project_id, batch=batch)

    def get_project_annotations(self, kognic_project_id: str, batch: str, annotation_type: str):
        """Gets annotations for a given project, batch name, and annotation type
        Args:
            kognic_project_id : Kognic Project Id
            batch: kognic batch id
            annotation_type: kognic annotation type

        Returns:
            List : Kognic annotation types
        """
        annotations = self.kognic_client.annotation.get_project_annotations(project=kognic_project_id,
                                                                            batch=batch,
                                                                            annotation_type=annotation_type)
        return annotations

    def verify_batch(self, kognic_project_id, batch_name):
        """Verifies if given batch name exists in kognic project
        Args:
            kognic_project_id : Kognic Project Id
            batch_name : Kognic Project Batch Name

        Returns:
            Boolean : True if exist, False if not
        """
        batches = [
            project_batch.title for project_batch in self.kognic_client.project.get_project_batches(kognic_project_id)]
        return batch_name in batches

    def create_batch(self, kognic_project_id, batch_name):
        """Creates batch in Kognic Project

        Args:
            kognic_project_id : Kognic Project Id
            batch_name : Batch Name
        """
        self.kognic_client.project.create_batch(kognic_project_id, batch_name)

    def upload_image(self, kognic_project_id, batch_name, labelling_type, file_path, file_name):  # pylint: disable=too-many-arguments
        """Uploads Image to Kognic Batch in the Kognic Project

        Args:
            kognic_project_id : Kognic Project Id
            batch_name : Batch Name
            labelling_type : Type of labelling(e.g. Splines)
            file_path :Image file path in s3
            file_name : Image file name
        """
        cameras = CamerasModel.Cameras(
            external_id=file_path,
            frame=CamerasModel.Frame(
                images=[InputModel.Image(filename=file_name, sensor_name="Voxel_export")]
            )
        )
        try:
            # upload
            self.kognic_client.cameras.create(
                cameras,
                project=kognic_project_id,
                batch=batch_name,
                annotation_types=labelling_type,
                dryrun=False)
        except Exception as e:  # pylint: disable=broad-exception-caught, invalid-name
            _logger.exception("Upload file failed: %s", str(e))
