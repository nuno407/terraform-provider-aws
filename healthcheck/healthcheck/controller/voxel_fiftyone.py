# type: ignore
# pylint: disable=too-few-public-methods, no-value-for-parameter, line-too-long
"""voxel 51 healthcheck controller module."""
from kink import inject

from base.model.artifacts import Artifact, RecorderType
from base.voxel.utils import determine_dataset_name
from healthcheck.exceptions import VoxelEntryNotPresent, VoxelEntryNotUnique
from healthcheck.model import S3Params
from healthcheck.voxel_client import VoxelEntriesGetter


@inject
class VoxelFiftyOneController:
    """Voxel fiftyone controller class."""

    def __init__(self,
                 s3_params: S3Params,
                 voxel_client: VoxelEntriesGetter):
        self.__s3_params = s3_params
        self.__voxel_client = voxel_client

    def _full_s3_path(self, tenant_name: str, file_key: str) -> str:
        """
        Given an s3 file name, appends the root folder to the key.

        Args:
            file_key (str): name of the file.

        Returns:
            str: The S3 key fot he file requested
        """
        return f"{tenant_name}/{file_key}"

    def is_fiftyone_entry_present_or_raise(self, artifact: Artifact) -> None:
        """
        Checks if the artifact specified is present in the dataset.
        Raises an exception if is either not present or is not unique.

        Args:
            artifact (Artifact): The artifact to check for.

        Raises:
            VoxelEntryNotPresent: If no record was found.
            VoxelEntryNotUnique: If exists more than 1 record.
        """
        art_id = artifact.artifact_id

        extension = "mp4"
        if artifact.recorder == RecorderType.SNAPSHOT:
            extension = "jpeg"

        dataset, _ = determine_dataset_name(
            artifact.tenant_id,
            artifact.recorder == RecorderType.SNAPSHOT
        )  # pylint: disable=no-value-for-parameter

        def get_key():
            return f"{self._full_s3_path(artifact.tenant_id, art_id)}_anonymized.{extension}"

        path = f"s3://{self.__s3_params.s3_bucket_anon}/{get_key()}"

        entries = self.__voxel_client.get_num_entries(path, dataset)
        if entries == 0:
            raise VoxelEntryNotPresent(art_id, f"Voxel entry for file path {path} does not exist in {dataset}")
        if entries > 1:
            raise VoxelEntryNotUnique(art_id, f"Multiple voxel entries ({entries}) found for file path {path}")
