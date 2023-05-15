""" Selector Tests. """
from datetime import datetime, timedelta
from unittest.mock import Mock, PropertyMock, call, patch

import pytest
from pytz import UTC

from base.model.artifacts import RecorderType, TimeWindow, VideoArtifact
from selector.selector import Selector


@pytest.mark.unit
class TestSelector():  # pylint: disable=too-few-public-methods
    """ Tests on Selector Component. """

    @patch("selector.selector.parse_artifact")
    def test_correct_footage_api_call_success(self, parse_artifact_mock: Mock):
        """Tests that the footage api is called correctly"""
        footage_api_wrapper = Mock()
        footage_api_wrapper.request_recorder = Mock()

        sqs_controller = Mock()
        sqs_controller.get_queue_url.return_value = "queue_url"
        sqs_controller.delete_message = Mock(return_value=None)

        video_artifact = VideoArtifact(
            tenant_id="tenant1",
            device_id="device1",
            recorder=RecorderType.TRAINING,
            timestamp=datetime.now(tz=UTC),
            end_timestamp=datetime.now(tz=UTC),
            stream_name="stream1",
            upload_timing=TimeWindow(
                start=datetime.now(tz=UTC) - timedelta(hours=1),
                end=datetime.now(tz=UTC))
        )
        message = {"ReceiptHandle": "1234", "Body": video_artifact.stringify()}
        sqs_controller.get_message.return_value = message
        parse_artifact_mock.return_value = video_artifact

        graceful_exit = Mock()
        type(graceful_exit).continue_running = PropertyMock(side_effect=[True, False])

        selector = Selector(
            footage_api_wrapper,
            sqs_controller
        )

        from_timestamp = int(video_artifact.timestamp.timestamp() * 1000)
        to_timestamp = int(video_artifact.end_timestamp.timestamp() * 1000)

        selector.run(graceful_exit)

        parse_artifact_mock.assert_called_once()
        footage_api_wrapper.request_recorder.assert_has_calls([
            call("TRAINING", video_artifact.device_id, from_timestamp, to_timestamp),
            call("TRAINING_MULTI_SNAPSHOT", video_artifact.device_id, from_timestamp, to_timestamp)
        ])
        sqs_controller.delete_message.assert_called_once_with(message)
