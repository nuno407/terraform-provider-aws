from datetime import datetime
from unittest.mock import Mock, call

import pytest

from healthcheck.checker.snapshot import SnapshotArtifactChecker
from healthcheck.exceptions import FailDocumentValidation, NotYetIngestedError
from healthcheck.model import Artifact, SnapshotArtifact
from healthcheck.voxel_client import VoxelDataset


@pytest.mark.unit
class TestSnapshotArtifactChecker:
    @pytest.mark.parametrize("input_artifact,is_matching_recording_ingested", [
        (SnapshotArtifact(
            uuid="uuid1",
            tenant_id="my_tenant1",
            device_id="my_device1",
            timestamp=datetime.now()
        ), True),
        (SnapshotArtifact(
            uuid="uuid2",
            tenant_id="my_tenant2",
            device_id="my_device2",
            timestamp=datetime.now()
        ), False)
    ])
    def test_run_healthcheck(self, input_artifact: Artifact, is_matching_recording_ingested: bool):
        s3_controller = Mock()
        s3_controller.is_s3_raw_file_present_or_raise = Mock()
        s3_controller.is_s3_anonymized_file_present_or_raise = Mock()

        db_controller = Mock()
        db_controller.is_signals_doc_valid_or_raise = Mock()
        if is_matching_recording_ingested:
            db_controller.is_recordings_doc_valid_or_raise = Mock()
        else:
            db_controller.is_recordings_doc_valid_or_raise = Mock(side_effect=FailDocumentValidation(
                input_artifact, message="error validating doc", json_path="$.recording_overview.source_videos"))
        db_controller.is_pipeline_execution_and_algorithm_output_doc_valid_or_raise = Mock()
        db_controller.is_data_status_complete_or_raise = Mock()

        voxel_fiftyone_controller = Mock()
        voxel_fiftyone_controller.is_fiftyone_entry_present_or_raise = Mock()

        snapshot_artifact_checker = SnapshotArtifactChecker(
            s3_controller=s3_controller,
            db_controller=db_controller,
            voxel_fiftyone_controller=voxel_fiftyone_controller
        )

        if is_matching_recording_ingested:
            snapshot_artifact_checker.run_healthcheck(input_artifact)
        else:
            with pytest.raises(NotYetIngestedError):
                snapshot_artifact_checker.run_healthcheck(input_artifact)
        db_controller.is_data_status_complete_or_raise.assert_called_once_with(
            input_artifact)
        snapshot_id = input_artifact.artifact_id

        s3_controller.is_s3_raw_file_present_or_raise.assert_called_once_with(
            f"{snapshot_id}.jpeg", input_artifact)
        s3_controller.is_s3_anonymized_file_present_or_raise(
            f"{snapshot_id}_anonymized.jpeg", input_artifact)

        db_controller.is_recordings_doc_valid_or_raise.assert_called_once_with(
            input_artifact)

        if is_matching_recording_ingested:
            voxel_fiftyone_controller.is_fiftyone_entry_present_or_raise.assert_called_once_with(
                input_artifact, VoxelDataset.SNAPSHOTS)
