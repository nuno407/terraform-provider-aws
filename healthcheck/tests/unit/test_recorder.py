from datetime import datetime
from unittest.mock import Mock, call

import pytest

from healthcheck.checker.training_recorder import \
    TrainingRecorderArtifactChecker
from healthcheck.model import VideoArtifact
from healthcheck.voxel_client import VoxelDataset


@pytest.mark.unit
class TestTrainingrecorderArtifactChecker:
    @pytest.mark.parametrize("input_artifact", [
        (
            VideoArtifact(
                tenant_id="my_tenant1",
                device_id="my_device1",
                stream_name="my_stream1_TrainingRecorder",
                footage_from=datetime.now(),
                footage_to=datetime.now()
            )
        ),
        (
            VideoArtifact(
                tenant_id="my_tenant2",
                device_id="my_device2",
                stream_name="my_stream2_TrainingRecorder",
                footage_from=datetime.now(),
                footage_to=datetime.now()
            )
        )
    ])
    def test_run_healthcheck(self, input_artifact: VideoArtifact):
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
        training_recorder_artifact_checker = TrainingRecorderArtifactChecker(
            blob_controller=blob_storage_controller,
            db_controller=db_controller,
            voxel_fiftyone_controller=voxel_fiftyone_controller
        )

        training_recorder_artifact_checker.run_healthcheck(input_artifact)
        db_controller.is_data_status_complete_or_raise.assert_called_once_with(input_artifact)
        video_id = input_artifact.artifact_id

        blob_storage_controller.is_s3_raw_file_presence_or_raise.assert_called_once_with(f"{video_id}.mp4", input_artifact)

        blob_storage_controller.is_s3_anonymized_file_present_or_raise.assert_has_calls(
            calls=[
                call(f"{video_id}_anonymized.mp4", input_artifact),
                call(f"{video_id}_chc.json", input_artifact)
            ]
        )

        db_controller.is_recordings_doc_valid_or_raise.assert_called_once_with(input_artifact)
        db_controller.is_pipeline_execution_and_algorithm_output_doc_valid_or_raise.assert_called_once_with(input_artifact)

        voxel_fiftyone_controller.is_fiftyone_entry_present_or_raise.assert_called_once_with(input_artifact, VoxelDataset.VIDEOS)
