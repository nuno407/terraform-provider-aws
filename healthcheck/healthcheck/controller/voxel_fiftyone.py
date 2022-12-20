"""voxel 51 healthcheck controller module."""
from kink import inject

from healthcheck.exceptions import VoxelEntryNotPresent, VoxelEntryNotUnique
from healthcheck.model import Artifact, ArtifactType, S3Params
from healthcheck.voxel_client import VoxelDataset, VoxelEntriesGetter


@inject
class VoxelFiftyOneController():
    """Voxel fifty one controller class."""

    def __init__(self,
                 s3_params: S3Params,
                 voxel_client: VoxelEntriesGetter):
        self.__s3_params = s3_params
        self.__voxel_client = voxel_client

    def _full_s3_path(self, file_key: str) -> str:
        """
        Given an s3 file name, appends the root folder to the key.

        Args:
            file_key (str): name of the file.

        Returns:
            str: The S3 key fot he file requested
        """
        return f"{self.__s3_params.s3_dir}/{file_key}"

    def is_fiftyone_entry_present_or_raise(self, artifact: Artifact, dataset: VoxelDataset) -> None:
        """
        Checks if the artifact specified is present in the dataset.
        Raises an exception if is either not present or is not unique.

        Args:
            artifact_id (str): The id of the artifactto check for.
            dataset (VoxelDataset): The dataset type to be checked.

        Raises:
            VoxelEntryNotPresent: If no record was found.
            VoxelEntryNotUnique: If exists more then 1 records.
        """
        artifact_id = artifact.artifact_id
        extension = "jpeg" if artifact.artifact_type == ArtifactType.SNAPSHOT else "mp4"
        s3_path = f"s3://{self.__s3_params.s3_bucket_anon}/{self._full_s3_path(artifact_id)}_anonymized.{extension}"

        entries = self.__voxel_client.get_num_entries(s3_path, dataset)
        if entries == 0:
            raise VoxelEntryNotPresent(
                artifact, f"Voxel entry for file path {s3_path} does not exist in {dataset.value}")
        if entries > 1:
            raise VoxelEntryNotUnique(
                artifact, f"Multiple voxel entries ({entries}) exists for file path {s3_path} does not exist")
