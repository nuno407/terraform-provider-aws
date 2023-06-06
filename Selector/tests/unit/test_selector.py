""" Selector Tests. """
from datetime import datetime, timedelta
from unittest.mock import Mock, PropertyMock, call, patch, MagicMock

import pytest
from pytz import UTC
from typing import Optional

from base.model.artifacts import RecorderType
from base.model.artifacts import RecorderType, SignalsArtifact, IMUArtifact, SnapshotArtifact, VideoArtifact, PreviewSignalsArtifact, Artifact
from selector.decision import Decision
from selector.selector import Selector


@pytest.mark.unit
class TestSelector():  # pylint: disable=too-few-public-methods
    """ Tests on Selector Component. """

    @patch("selector.selector.parse")
    @patch("selector.selector.parse_artifact")
    def test_preview_footage_api_call_success(self, parse_artifact_mock: Mock, parse_mock: Mock):
        """Tests that the footage api is called correctly"""
        footage_api_wrapper = Mock()
        footage_api_wrapper.request_recorder = Mock()

        sqs_controller = Mock()
        sqs_controller.get_queue_url.return_value = "queue_url"
        message = {
            "Body": Mock()
        }
        sqs_controller.get_message = Mock(return_value=message)
        sqs_controller.delete_message = Mock(return_value=None)

        from_ts = datetime.now(tz=UTC) - timedelta(minutes=5)
        to_ts = datetime.now(tz=UTC)
        video_artifact = MagicMock(spec=PreviewSignalsArtifact)
        video_artifact.timestamp = from_ts
        video_artifact.end_timestamp = to_ts
        video_artifact.s3_path = "test_s3_path"
        video_artifact.device_id = "test_device"
        parse_artifact_mock.return_value = video_artifact

        s3_controller = Mock()
        s3_controller.get_s3_path_parts.return_value = ("bucket", "key")
        s3_controller.download_file.return_value = b"{\"foo\": \"bar\"}"

        preview_metadata = Mock()
        parse_mock.return_value = preview_metadata

        evaluator = Mock()
        evaluator.evaluate.return_value = [Decision(RecorderType.TRAINING, from_ts, to_ts)]

        graceful_exit = Mock()
        type(graceful_exit).continue_running = PropertyMock(side_effect=[True, False])

        selector = Selector(
            s3_controller,
            footage_api_wrapper,
            sqs_controller,
            evaluator
        )

        selector.run(graceful_exit)

        parse_artifact_mock.assert_called_once_with(message["Body"])
        s3_controller.get_s3_path_parts.assert_called_once_with(video_artifact.s3_path)
        s3_controller.download_file.assert_called_once_with("bucket", "key")
        parse_mock.assert_called_once_with({"foo": "bar"})
        evaluator.evaluate.assert_called_once_with(preview_metadata, video_artifact)
        footage_api_wrapper.request_recorder.assert_has_calls([
            call(RecorderType.TRAINING, video_artifact.device_id, from_ts, to_ts)
        ])
        sqs_controller.delete_message.assert_called_once_with(message)

    @patch("selector.selector.parse")
    @patch("selector.selector.parse_artifact")
    def test_correct_interior_footage_api_call_success(
            self, parse_artifact_mock: Mock, parse_mock: Mock):
        """Tests that the footage api is called correctly"""
        footage_api_wrapper = Mock()
        footage_api_wrapper.request_recorder = Mock()

        sqs_controller = Mock()
        sqs_controller.get_queue_url.return_value = "queue_url"
        message = {
            "Body": Mock()
        }
        sqs_controller.get_message = Mock(return_value=message)
        sqs_controller.delete_message = Mock(return_value=None)

        from_ts = datetime.now(tz=UTC) - timedelta(minutes=5)
        to_ts = datetime.now(tz=UTC)
        video_artifact = MagicMock(spec=VideoArtifact)
        video_artifact.timestamp = from_ts
        video_artifact.end_timestamp = to_ts
        video_artifact.s3_path = "test_s3_path"
        video_artifact.device_id = "datanauts"
        video_artifact.recorder = RecorderType.INTERIOR
        parse_artifact_mock.return_value = video_artifact

        s3_controller = Mock()
        evaluator = Mock()

        graceful_exit = Mock()
        type(graceful_exit).continue_running = PropertyMock(side_effect=[True, False])

        selector = Selector(
            s3_controller,
            footage_api_wrapper,
            sqs_controller,
            evaluator
        )

        selector.run(graceful_exit)

        parse_artifact_mock.assert_called_once_with(message["Body"])
        footage_api_wrapper.request_recorder.assert_has_calls([
            call(RecorderType.TRAINING, video_artifact.device_id, from_ts, to_ts)
        ])
        sqs_controller.delete_message.assert_called_once_with(message)

    @patch("selector.selector.parse")
    @patch("selector.selector.parse_artifact")
    @pytest.mark.parametrize("recorder_type,artifact_type", [
        (RecorderType.TRAINING, VideoArtifact),
        (RecorderType.FRONT, VideoArtifact),
        (RecorderType.SNAPSHOT, SnapshotArtifact),
        (None, SignalsArtifact),
        (None, IMUArtifact),
    ])
    def test_incorrect_artifact(self, parse_artifact_mock: Mock, parse_mock: Mock,
                                recorder_type: Optional[RecorderType], artifact_type: Artifact):
        """Ensures that some artifacts do not request footages"""
        footage_api_wrapper = Mock()
        footage_api_wrapper.request_recorder = Mock()

        sqs_controller = Mock()
        sqs_controller.get_queue_url.return_value = "queue_url"
        message = {
            "Body": Mock()
        }
        sqs_controller.get_message = Mock(return_value=message)

        artifact = MagicMock(spec=artifact_type)
        artifact.recorder = recorder_type
        parse_artifact_mock.return_value = artifact

        s3_controller = Mock()
        evaluator = Mock()
        graceful_exit = Mock()
        type(graceful_exit).continue_running = PropertyMock(side_effect=[True, False])

        selector = Selector(
            s3_controller,
            footage_api_wrapper,
            sqs_controller,
            evaluator
        )

        selector.run(graceful_exit)

        parse_artifact_mock.assert_called_once_with(message["Body"])
        footage_api_wrapper.request_recorder.assert_not_called()
