""" Voxel Video Model """
from enum import Enum
import logging
from typing import Any
from datetime import datetime
import fiftyone as fo
from base.voxel.functions import get_anonymized_path_from_raw
from base.model.artifacts import S3VideoArtifact, PipelineProcessingStatus
from base.model.artifacts.upload_rule_model import VideoUploadRule
from artifact_api.voxel.voxel_embedded_models import UploadVideoRuleEmbeddedDocument
from artifact_api.voxel.voxel_base_models import VoxelField, VoxelSample

_logger = logging.getLogger(__name__)

# pylint: disable=duplicate-code


class VoxelVideo(VoxelSample):  # pylint: disable=too-few-public-methods
    """
    Class responsible for parsing the message that comes from the API to populate video
    """

    # Please add here all necessary fields for voxel
    class Fields(Enum):
        """ Enum containing all video voxel fields. """
        VIDEO_ID = VoxelField(field_name="video_id",
                              field_type=fo.core.fields.StringField)
        TENANT_ID = VoxelField(field_name="tenant_id",
                               field_type=fo.core.fields.StringField)
        DEVICE_ID = VoxelField(field_name="device_id",
                               field_type=fo.core.fields.StringField)
        RECORDING_TIME = VoxelField(
            field_name="recording_time", field_type=fo.core.fields.DateTimeField)
        HOUR = VoxelField(field_name="hour",
                          field_type=fo.core.fields.IntField)
        DAY = VoxelField(field_name="day", field_type=fo.core.fields.IntField)
        MONTH = VoxelField(field_name="month",
                           field_type=fo.core.fields.IntField)
        YEAR = VoxelField(field_name="year",
                          field_type=fo.core.fields.IntField)
        RECORDING_DURATION = VoxelField(
            field_name="recording_duration", field_type=fo.core.fields.FloatField)
        RESOLUTION = VoxelField(field_name="resolution",
                                field_type=fo.core.fields.StringField)
        SNAPSHOTS_PATHS = VoxelField(
            field_name="snapshots_paths",
            field_type=fo.core.fields.ListField,
            field_subtype=fo.core.fields.StringField)
        NUM_SNAPSHOTS = VoxelField(
            field_name="num_snapshots", field_type=fo.core.fields.IntField)
        RULES = VoxelField(
            field_name="rules",
            field_type=fo.core.fields.ListField,
            field_subtype=fo.core.fields.EmbeddedDocumentField(fo.DynamicEmbeddedDocument))
        AGREGATED_METADATA = VoxelField(
            field_name="aggregated_metadata",
            field_type=fo.EmbeddedDocumentField,
            field_embedded_type=fo.DynamicEmbeddedDocument
        )
        DATA_STATUS = VoxelField(
            field_name="data_status", field_type=fo.core.fields.StringField)
        LAST_UPDATED = VoxelField(
            field_name="last_updated", field_type=fo.core.fields.DateTimeField)

    @classmethod
    def _get_fields(cls) -> list[VoxelField]:
        return [item.value for item in cls.Fields]

    @classmethod
    def video_sample(cls, artifact: S3VideoArtifact, dataset: fo.Dataset, correlated_raw_filepaths: list[str]) -> None:
        """
        Creates or updates a video sample
        """
        anonymized_filepath = get_anonymized_path_from_raw(
            filepath=artifact.raw_s3_path)
        correlated_anonymized_filepaths = [get_anonymized_path_from_raw(
            raw_path) for raw_path in correlated_raw_filepaths]

        values_to_set: dict[VoxelField, Any] = {
            cls.Fields.VIDEO_ID.value: artifact.artifact_id,
            cls.Fields.TENANT_ID.value: artifact.tenant_id,
            cls.Fields.DEVICE_ID.value: artifact.device_id,
            cls.Fields.RECORDING_TIME.value: artifact.timestamp,
            cls.Fields.HOUR.value: None if artifact.timestamp is None else artifact.timestamp.hour,
            cls.Fields.DAY.value: None if artifact.timestamp is None else artifact.timestamp.day,
            cls.Fields.MONTH.value: None if artifact.timestamp is None else artifact.timestamp.month,
            cls.Fields.YEAR.value: None if artifact.timestamp is None else artifact.timestamp.year,
            cls.Fields.RECORDING_DURATION.value: artifact.duration,
            cls.Fields.RESOLUTION.value: None if artifact.resolution is None else f"{artifact.resolution.width}x{artifact.resolution.height}",  # pylint: disable=line-too-long
            cls.Fields.SNAPSHOTS_PATHS.value: VoxelSample._append_to_list("snapshots_paths", correlated_anonymized_filepaths),  # pylint: disable=line-too-long
            cls.Fields.NUM_SNAPSHOTS.value: len(correlated_anonymized_filepaths),
        }
        cls._upsert_sample(artifact.tenant_id, dataset,
                           anonymized_filepath, values_to_set)

    @classmethod
    def updates_correlation(cls, correlated_raw_filepaths: list[str], raw_filepath: str, dataset_name: str) -> None:
        """
        Updates all snapshots that have a correlation with this video
        """
        anonymized_filepath = get_anonymized_path_from_raw(raw_filepath)
        anonymized_correlated = [get_anonymized_path_from_raw(
            raw_path) for raw_path in correlated_raw_filepaths]
        cls._update_correlation(anonymized_correlated, anonymized_filepath,
                                dataset_name, correlation_field="snapshots_paths")

    @classmethod
    def attach_rule_to_video(cls, dataset: fo.Dataset, rule: VideoUploadRule) -> None:
        """ Attach a upload rule to the referred snapshot. """
        db_rule = UploadVideoRuleEmbeddedDocument(
            name=rule.rule.rule_name,
            version=rule.rule.rule_version,
            origin=rule.rule.origin.value,
            footage_from=rule.footage_from,
            footage_to=rule.footage_to)
        anonymized_filepath = get_anonymized_path_from_raw(
            filepath=rule.raw_file_path)
        values_to_set: dict[VoxelField, Any] = {
            cls.Fields.RULES.value: VoxelSample._append_to_list(
                "rules", [fo.DynamicEmbeddedDocument(**db_rule.model_dump())])
        }
        cls._upsert_sample(
            tenant_id=rule.tenant,
            dataset=dataset,
            anonymized_filepath=anonymized_filepath,
            values_to_set=values_to_set)

    @classmethod
    def attach_pipeline_processing_status_to_video(
            cls,
            dataset: fo.Dataset,
            pipeline_status: PipelineProcessingStatus,
            last_updated: datetime):
        """ Attach a pipeline processing status to the referred snapshot. """
        anonymized_filepath = get_anonymized_path_from_raw(
            filepath=pipeline_status.s3_path)

        values_to_set: dict[VoxelField, Any] = {
            cls.Fields.DATA_STATUS.value: pipeline_status.processing_status.value,
            cls.Fields.LAST_UPDATED.value: last_updated
        }

        cls._upsert_sample(
            tenant_id=pipeline_status.tenant_id,
            dataset=dataset,
            anonymized_filepath=anonymized_filepath,
            values_to_set=values_to_set)

    @classmethod
    def load_device_aggregated_metadata(cls,
                                        dataset: fo.Dataset,
                                        tenant: str,
                                        raw_s3_path: str,
                                        agregated_metrics: dict[str, str | int | float | bool]):
        """ Load device agregated metadata. """
        anonymized_filepath = get_anonymized_path_from_raw(filepath=raw_s3_path)

        _logger.debug("Anonymized path calculated : %s", anonymized_filepath)
        _logger.debug("Inserting aggregated_metadata : %s",str(agregated_metrics))
        values_to_set: dict[VoxelField, fo.DynamicEmbeddedDocument] = {
            cls.Fields.AGREGATED_METADATA.value: fo.DynamicEmbeddedDocument(**agregated_metrics)
        }
        cls._upsert_sample(
            tenant_id=tenant,
            dataset=dataset,
            anonymized_filepath=anonymized_filepath,
            values_to_set=values_to_set)

        _logger.info("Device aggregated metadata inserted successfully")
