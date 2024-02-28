""" Voxel Snapshot Model """
from enum import Enum
from typing import Any
import logging
from datetime import datetime
import fiftyone as fo
from base.voxel.constants import POSE_LABEL, CLASSIFICATION_LABEL
from base.voxel.functions import get_anonymized_path_from_raw
from base.model.artifacts import SnapshotArtifact, PipelineProcessingStatus, AnonymizationResult
from base.model.artifacts.upload_rule_model import SnapshotUploadRule
from artifact_api.voxel.voxel_embedded_models import UploadSnapshotRuleEmbeddedDocument
from artifact_api.voxel.voxel_base_models import VoxelSample, VoxelField
from artifact_api.voxel.voxel_metadata_transformer import VoxelMetadataFrameFields

# pylint: disable=duplicate-code
_logger = logging.getLogger(__name__)


class VoxelSnapshot(VoxelSample):  # pylint: disable=too-few-public-methods
    """
    Class responsible for parsing the message that comes from the API to populate snapshots
    """
    # Please add here all necessary fields for voxel
    class Fields(Enum):
        """ Enum containing all snapshot voxel fields. """
        VIDEO_ID = VoxelField(field_name="video_id",
                              field_type=fo.core.fields.StringField)
        TENANT_ID = VoxelField(field_name="tenant_id",
                               field_type=fo.core.fields.StringField)
        DEVICE_ID = VoxelField(field_name="device_id",
                               field_type=fo.core.fields.StringField)
        RECORDING_TIME = VoxelField(
            field_name="recording_time", field_type=fo.core.fields.DateTimeField)
        KEYPOINTS = VoxelField(
            field_name=POSE_LABEL,
            field_type=fo.EmbeddedDocumentField,
            field_embedded_type=fo.Keypoints
        )
        CLASSIFICATIONS = VoxelField(
            field_name=CLASSIFICATION_LABEL,
            field_type=fo.EmbeddedDocumentField,
            field_embedded_type=fo.Classifications
        )
        SOURCE_VIDEOS = VoxelField(
            field_name="source_videos",
            field_type=fo.core.fields.ListField,
            field_subtype=fo.core.fields.StringField
        )
        RULES = VoxelField(
            field_name="rules",
            field_type=fo.core.fields.ListField,
            field_subtype=fo.core.fields.EmbeddedDocumentField(
                fo.DynamicEmbeddedDocument)
        )
        DATA_STATUS = VoxelField(
            field_name="data_status", field_type=fo.core.fields.StringField)
        LAST_UPDATED = VoxelField(
            field_name="last_updated", field_type=fo.core.fields.DateTimeField)

    @classmethod
    def _get_fields(cls) -> list[VoxelField]:
        return [item.value for item in cls.Fields]

    @classmethod
    def snapshot_sample(cls, artifact: SnapshotArtifact, dataset: fo.Dataset,
                        correlated_raw_filepaths: list[str]) -> None:
        """
        Creates or updates a snapshot sample
        """
        anonymized_filepath = get_anonymized_path_from_raw(
            filepath=artifact.raw_s3_path)
        correlated_anonymized_filepaths = [get_anonymized_path_from_raw(
            raw_path) for raw_path in correlated_raw_filepaths]

        values_to_set: dict[VoxelField, Any] = {
            cls.Fields.VIDEO_ID.value: artifact.artifact_id,
            cls.Fields.TENANT_ID.value: artifact.tenant_id,
            cls.Fields.DEVICE_ID.value: artifact.device_id,
            cls.Fields.SOURCE_VIDEOS.value: VoxelSample._append_to_list("source_videos", correlated_anonymized_filepaths),  # pylint: disable=line-too-long
            cls.Fields.RECORDING_TIME.value: artifact.timestamp
        }
        cls._upsert_sample(artifact.tenant_id, dataset,
                           anonymized_filepath, values_to_set)

    @classmethod
    def updates_correlation(cls, raw_correlated_filepath: list[str], raw_filepath: str, dataset_name: str) -> None:
        """
        Updates all snapshots that have a correlation with this video
        """
        anonymized_filepath = get_anonymized_path_from_raw(raw_filepath)
        anonymized_correlated = [get_anonymized_path_from_raw(
            raw_path) for raw_path in raw_correlated_filepath]
        cls._update_correlation(anonymized_correlated, anonymized_filepath,
                                dataset_name, correlation_field="source_videos")

    @classmethod
    def attach_rule_to_snapshot(cls, dataset: fo.Dataset, rule: SnapshotUploadRule) -> None:
        """ Attach a upload rule to the referred snapshot. """
        db_rule = UploadSnapshotRuleEmbeddedDocument(
            name=rule.rule.rule_name,
            version=rule.rule.rule_version,
            origin=rule.rule.origin.value,
            snapshot_timestamp=rule.snapshot_timestamp)
        anonymized_filepath = get_anonymized_path_from_raw(
            filepath=rule.raw_file_path)
        values_to_set: dict[VoxelField, Any] = {cls.Fields.RULES.value: VoxelSample._append_to_list(
            "rules", [fo.DynamicEmbeddedDocument(**db_rule.model_dump())])}
        cls._upsert_sample(
            tenant_id=rule.tenant,
            dataset=dataset,
            anonymized_filepath=anonymized_filepath,
            values_to_set=values_to_set)

    @classmethod
    def load_metadata(
            cls,
            dataset: fo.Dataset,
            sample_anonymized_s3_path: str,
            tenant_id: str,
            field_values: VoxelMetadataFrameFields):
        """ Loads the metadata from the signals artifact into the dataset."""

        values_to_set: dict[VoxelField, Any] = {
            cls.Fields.KEYPOINTS.value: field_values.keypoints,
            cls.Fields.CLASSIFICATIONS.value: field_values.classifications
        }

        cls._upsert_sample(
            tenant_id=tenant_id,
            dataset=dataset,
            anonymized_filepath=sample_anonymized_s3_path,
            values_to_set=values_to_set)

    @classmethod
    def attach_pipeline_processing_status_to_snapshot(
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
    def update_processing_status_anonymization(cls,
                                               dataset: fo.Dataset,
                                               message: AnonymizationResult,
                                               last_updated: datetime):
        """ Updates the processing status of the snapshot after anonymization."""

        values_to_set: dict[VoxelField, Any] = {
            cls.Fields.DATA_STATUS.value: message.processing_status.value,
            cls.Fields.LAST_UPDATED.value: last_updated
        }
        cls._upsert_sample(
            tenant_id=message.tenant_id,
            dataset=dataset,
            anonymized_filepath=message.s3_path,
            values_to_set=values_to_set)
        _logger.info("Snapshot anonymization status updated successfully")
