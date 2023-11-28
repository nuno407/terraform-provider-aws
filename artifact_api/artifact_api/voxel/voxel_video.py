""" Voxel Video Model """
from typing import List
import fiftyone as fo
from base.voxel.functions import get_anonymized_path_from_raw
from base.model.artifacts import S3VideoArtifact
from artifact_api.voxel.voxel_base_models import VoxelField, VoxelSample


# pylint: disable=duplicate-code
class VoxelVideo(VoxelSample):  # pylint: disable=too-few-public-methods
    """
    Class responsible for parsing the message that comes from the API to populate video
    """

    # Please add here all necessary fields for voxel
    fields = [
        VoxelField(field_name="video_id",
                   field_type=fo.core.fields.StringField,
                   field_value=lambda ctx: ctx.artifact.artifact_id),
        VoxelField(field_name="tenant_id",
                   field_type=fo.core.fields.StringField,
                   field_value=lambda ctx: ctx.artifact.tenant_id),
        VoxelField(field_name="device_id",
                   field_type=fo.core.fields.StringField,
                   field_value=lambda ctx: ctx.artifact.device_id),
        VoxelField(field_name="recording_time",
                   field_type=fo.core.fields.DateTimeField,
                   field_value=lambda ctx: ctx.artifact.timestamp),
        VoxelField(field_name="hour",
                   field_type=fo.core.fields.IntField,
                   field_value=lambda ctx: None if ctx.artifact.timestamp is None else ctx.artifact.timestamp.hour),
        VoxelField(field_name="day",
                   field_type=fo.core.fields.IntField,
                   field_value=lambda ctx: None if ctx.artifact.timestamp is None else ctx.artifact.timestamp.day),
        VoxelField(field_name="month",
                   field_type=fo.core.fields.IntField,
                   field_value=lambda ctx: None if ctx.artifact.timestamp is None else ctx.artifact.timestamp.month),
        VoxelField(field_name="year",
                   field_type=fo.core.fields.IntField,
                   field_value=lambda ctx: None if ctx.artifact.timestamp is None else ctx.artifact.timestamp.year),
        VoxelField(field_name="recording_duration",
                   field_type=fo.core.fields.FloatField,
                   field_value=lambda ctx: ctx.artifact.duration),
        VoxelField(field_name="resolution",
                   field_type=fo.core.fields.StringField,
                   field_value=lambda ctx: None if ctx.artifact.resolution is None else f"{ctx.artifact.resolution.width}x{ctx.artifact.resolution.height}"),  # pylint: disable=line-too-long
        VoxelField(field_name="snapshots_paths",
                   field_type=fo.core.fields.ListField,
                   field_subtype=fo.core.fields.StringField,
                   field_value=VoxelSample._correlated_paths("snapshots_paths")),
        VoxelField(field_name="num_snapshots",
                   field_type=fo.core.fields.IntField,
                   field_value=lambda ctx: len(ctx.correlated_anonymized_filepaths))
    ]

    @classmethod
    def video_sample(cls, artifact: S3VideoArtifact, dataset: fo.Dataset, correlated_raw_filepaths: list[str]) -> None:
        """
        Creates or updates a video sample
        """
        cls._create_sample(artifact, dataset, correlated_raw_filepaths)

    @classmethod
    def updates_correlation(cls, correlated_raw_filepaths: List[str], raw_filepath: str, dataset_name: str) -> None:
        """
        Updates all snapshots that have a correlation with this video
        """
        anonymized_filepath = get_anonymized_path_from_raw(raw_filepath)
        anonymized_correlated = [get_anonymized_path_from_raw(raw_path) for raw_path in correlated_raw_filepaths]
        cls._update_correlation(anonymized_correlated, anonymized_filepath,
                                dataset_name, correlation_field="snapshots_paths")
