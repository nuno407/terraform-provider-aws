from datetime import datetime
from unittest.mock import Mock, call

import pytest
from pytz import UTC

from base.model.artifacts import RecorderType, TimeWindow, VideoArtifact
from healthcheck.checker.training_recorder import \
    TrainingRecorderArtifactChecker

common_video_attributes = {
    "recorder": RecorderType.TRAINING,
    "timestamp": datetime.now(tz=UTC),
    "end_timestamp": datetime.now(tz=UTC),
    "upload_timing": TimeWindow(
        start=datetime.now(tz=UTC),
        end=datetime.now(tz=UTC))
}


@pytest.mark.unit
class TestTrainingrecorderArtifactChecker:
    @pytest.mark.parametrize("input_artifact", [
        (
            VideoArtifact(
                tenant_id="my_tenant1",
                device_id="my_device1",
                stream_name="my_stream1_TrainingRecorder",
                **common_video_attributes
            )
        ),
        (
            VideoArtifact(
                tenant_id="my_tenant2",
                device_id="my_device2",
                stream_name="my_stream2_TrainingRecorder",
                **common_video_attributes
            )
        )
    ])
    def test_run_healthcheck(self, input_artifact: VideoArtifact):
        s3_controller = Mock()
        s3_controller.is_s3_raw_file_present_or_raise = Mock()
        s3_controller.is_s3_anonymized_file_present_or_raise = Mock()

        db_controller = Mock()
        db_controller.is_signals_doc_valid_or_raise = Mock()
        db_controller.is_recordings_doc_valid_or_raise = Mock()
        db_controller.is_pipeline_execution_and_algorithm_output_doc_valid_or_raise = Mock()
        db_controller.is_data_status_complete_or_raise = Mock()

        voxel_fiftyone_controller = Mock()
        voxel_fiftyone_controller.is_fiftyone_entry_present_or_raise = Mock()
        training_recorder_artifact_checker = TrainingRecorderArtifactChecker(
            s3_utils=s3_controller,
            db_controller=db_controller,
            voxel_fiftyone_controller=voxel_fiftyone_controller
        )

        training_recorder_artifact_checker.run_healthcheck(input_artifact)
        db_controller.is_data_status_complete_or_raise.assert_called_once_with(
            input_artifact)
        video_id = input_artifact.artifact_id

        s3_controller.is_s3_raw_file_present_or_raise.assert_called_once_with(
            f"{video_id}.mp4", input_artifact)

        s3_controller.is_s3_anonymized_file_present_or_raise.assert_has_calls(
            calls=[
                call(f"{video_id}_anonymized.mp4", input_artifact),
                call(f"{video_id}_chc.json", input_artifact)
            ]
        )

        db_controller.is_recordings_doc_valid_or_raise.assert_called_once_with(
            input_artifact)
        db_controller.is_pipeline_execution_and_algorithm_output_doc_valid_or_raise.assert_called_once_with(
            input_artifact)

        voxel_fiftyone_controller.is_fiftyone_entry_present_or_raise.assert_called_once_with(
            input_artifact)
