# type: ignore
# pylint: disable=too-few-public-methods, no-value-for-parameter, line-too-long
"""voxel 51 healthcheck controller module."""
from kink import inject

from base.model.artifacts import Artifact, RecorderType
from healthcheck.exceptions import VoxelEntryNotPresent, VoxelEntryNotUnique
from healthcheck.model import S3Params
from healthcheck.tenant_config import DatasetMappingConfig
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

    @inject
    def _determine_dataset_name(self, tenant_id: str, recorder_type: RecorderType,
                                mapping_config: DatasetMappingConfig):
        """
        Checks in config if tenant gets its own dataset or if it is part of the default dataset.
        Dedicated dataset names are prefixed with the tag given in the config.
        The tag is not added to the default dataset.

        Args:
            tenant_id (str): Name of the tenant
            artifact_type (RecorderType): Either video or snapshot
            mapping_config (DatasetMappingConfig): Config with mapping information about the tenants

        Returns:
            the resulting dataset name and the tags which should be added on dataset creation
        """
        # The root dir on the S3 bucket always is the tenant name

        dataset_name = mapping_config.default_dataset

        if tenant_id in mapping_config.create_dataset_for:
            dataset_name = f"{mapping_config.tag}-{tenant_id}"

        if recorder_type == RecorderType.SNAPSHOT:
            dataset_name += "_snapshots"

        return dataset_name

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

        dataset = self._determine_dataset_name(artifact.tenant_id, artifact.recorder)

        def get_key():
            return f"{self._full_s3_path(artifact.tenant_id, art_id)}_anonymized.{extension}"

        path = f"s3://{self.__s3_params.s3_bucket_anon}/{get_key()}"

        entries = self.__voxel_client.get_num_entries(path, dataset)
        if entries == 0:
            raise VoxelEntryNotPresent(art_id, f"Voxel entry for file path {path} does not exist in {dataset}")
        if entries > 1:
            raise VoxelEntryNotUnique(art_id, f"Multiple voxel entries ({entries}) found for file path {path}")
