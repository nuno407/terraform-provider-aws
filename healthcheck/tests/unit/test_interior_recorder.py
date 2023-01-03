import pytest
from unittest.mock import Mock, call
from healthcheck.checker.interior_recorder import InteriorRecorderArtifactChecker
from healthcheck.model import VideoArtifact, Artifact
from healthcheck.voxel_client import VoxelDataset

from datetime import datetime

@pytest.mark.unit
class TestInteriorRecorderArtifactChecker:
    @pytest.mark.parametrize("input_artifact", [
        (VideoArtifact(
            tenant_id="my_tenant1",
            device_id="my_device1",
            stream_name="my_stream1_InteriorRecorder",
            footage_from=datetime.now(),
            footage_to=datetime.now()
        )),
        (VideoArtifact(
            tenant_id="my_tenant2",
            device_id="my_device2",
            stream_name="my_stream2_InteriorRecorder",
            footage_from=datetime.now(),
            footage_to=datetime.now()
        ))
    ])
    def test_run_healthcheck(self, input_artifact: Artifact):
        blob_storage_controller = Mock()
        blob_storage_controller.is_s3_raw_file_presence_or_raise = Mock()
        blob_storage_controller.is_s3_anonymized_file_present_or_raise = Mock()

        db_controller = Mock()
        db_controller.is_signals_doc_valid_or_raise = Mock()
        db_controller.is_recordings_doc_valid_or_raise = Mock()
        db_controller.is_pipeline_execution_and_algorithm_output_doc_valid_or_raise = Mock()
        db_controller.is_data_status_complete_or_raise = Mock()

        voxel_fiftyone_controller = Mock()
        voxel_fiftyone_controller.is_fiftyone_entry_present_or_raise = Mock()

        interior_recorder_artifact_checker = InteriorRecorderArtifactChecker(
            blob_controller=blob_storage_controller,
            db_controller=db_controller,
            voxel_fiftyone_controller=voxel_fiftyone_controller)

        interior_recorder_artifact_checker.run_healthcheck(input_artifact)
        db_controller.is_data_status_complete_or_raise.assert_called_once_with(input_artifact)
        artifact_id = input_artifact.artifact_id

        blob_storage_controller.is_s3_raw_file_presence_or_raise.assert_has_calls(
            calls=[
                call(f"{artifact_id}.mp4", input_artifact),
                call(f"{artifact_id}_signals.json", input_artifact),
                call(f"{artifact_id}_metadata_full.json", input_artifact)
            ]
        )

        blob_storage_controller.is_s3_anonymized_file_present_or_raise.assert_has_calls(
            calls=[
                call(f"{artifact_id}_anonymized.mp4", input_artifact),
                call(f"{artifact_id}_chc.json", input_artifact)
            ]
        )

        db_controller.is_recordings_doc_valid_or_raise.assert_called_once_with(input_artifact)
        db_controller.is_signals_doc_valid_or_raise.assert_called_once_with(input_artifact)
        db_controller.is_pipeline_execution_and_algorithm_output_doc_valid_or_raise.assert_called_once_with(input_artifact)

        voxel_fiftyone_controller.is_fiftyone_entry_present_or_raise.assert_called_once_with(input_artifact, VoxelDataset.VIDEOS)
