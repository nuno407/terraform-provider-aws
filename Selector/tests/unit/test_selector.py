""" Selector Tests. """
from datetime import datetime, timedelta, timezone
from typing import Optional
from unittest.mock import MagicMock, Mock, PropertyMock, call, patch

import pytest
from freezegun import freeze_time

from base.model.artifacts import (Artifact, CameraBlockedOperatorArtifact, IMUArtifact,
                                  OperatorArtifact, PeopleCountOperatorArtifact,
                                  PreviewSignalsArtifact, RecorderType, SignalsArtifact,
                                  S3VideoArtifact, SnapshotArtifact,
                                  SOSOperatorArtifact, VideoArtifact)
from selector.config import SelectorConfig
from selector.decision import Decision
from selector.selector import Selector
from selector.model import DBDecision


@pytest.mark.unit
class TestSelector():  # pylint: disable=too-few-public-methods
    """ Tests on Selector Component. """

    @patch("selector.selector.parse")
    @patch("selector.selector.parse_artifact")
    def test_preview_footage_api_call_success(self, parse_artifact_mock: Mock, parse_mock: Mock):
        """Tests that the footage api is called correctly"""
        footage_api_wrapper = Mock()
        footage_api_wrapper.request_recorder = Mock(return_value="foo_footage_id")

        sqs_controller = Mock()
        sqs_controller.get_queue_url.return_value = "queue_url"
        message = {
            "Body": Mock()
        }
        sqs_controller.get_message = Mock(return_value=message)
        sqs_controller.delete_message = Mock(return_value=None)

        from_ts = datetime.now(timezone.utc) - timedelta(minutes=5)
        to_ts = datetime.now(timezone.utc)
        video_artifact = MagicMock(spec=PreviewSignalsArtifact)
        video_artifact.timestamp = from_ts
        video_artifact.end_timestamp = to_ts
        video_artifact.s3_path = "test_s3_path"
        video_artifact.device_id = "test_device"
        video_artifact.tenant_id = "test_tenant"
        parse_artifact_mock.return_value = video_artifact

        s3_controller = Mock()
        s3_controller.get_s3_path_parts.return_value = ("bucket", "key")
        s3_controller.download_file.return_value = b"{\"foo\": \"bar\"}"

        preview_metadata = Mock()
        parse_mock.return_value = preview_metadata

        evaluator = Mock()
        correlator = Mock()
        evaluator.evaluate.return_value = [Decision("rule_name_one", "1.0.0", RecorderType.TRAINING, from_ts, to_ts),
                                           Decision("rule_name_two", "2.0.0", RecorderType.TRAINING, from_ts, to_ts)]
        DBDecision.save_db_decision = Mock()  # type: ignore
        config = Mock()
        graceful_exit = Mock()
        type(graceful_exit).continue_running = PropertyMock(side_effect=[True, False])

        selector = Selector(
            s3_controller,
            footage_api_wrapper,
            sqs_controller,
            config,
            evaluator,
            correlator
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
        assert DBDecision.save_db_decision.call_count == 2
        sqs_controller.delete_message.assert_called_once_with(message)

    @patch("selector.selector.parse_artifact")
    def test_correct_interior_footage_api_call_success(self, parse_artifact_mock: Mock):
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

        from_ts = datetime.now(timezone.utc) - timedelta(minutes=5)
        to_ts = datetime.now(timezone.utc)
        video_artifact = MagicMock(spec=VideoArtifact)
        video_artifact.timestamp = from_ts
        video_artifact.end_timestamp = to_ts
        video_artifact.s3_path = "test_s3_path"
        video_artifact.device_id = "datanauts"
        video_artifact.tenant_id = "test_tenant"
        video_artifact.recorder = RecorderType.INTERIOR
        parse_artifact_mock.return_value = video_artifact

        s3_controller = Mock()
        evaluator = Mock()
        correlator = Mock()
        config = Mock()
        graceful_exit = Mock()
        type(graceful_exit).continue_running = PropertyMock(side_effect=[True, False])

        selector = Selector(
            s3_controller,
            footage_api_wrapper,
            sqs_controller,
            config,
            evaluator,
            correlator
        )

        selector.run(graceful_exit)

        parse_artifact_mock.assert_called_once_with(message["Body"])
        footage_api_wrapper.request_recorder.assert_has_calls([
            call(RecorderType.TRAINING, video_artifact.device_id, from_ts, to_ts)
        ])
        sqs_controller.delete_message.assert_called_once_with(message)

    @freeze_time(datetime.now(timezone.utc))
    @patch("selector.selector.parse_artifact")
    @pytest.mark.parametrize("event_timestamp_delta", [timedelta(seconds=0),
                                                       timedelta(seconds=600)])
    def test_process_sav_operator(self, parse_artifact_mock: Mock,
                                  event_timestamp_delta: timedelta):
        """Tests that the footage api is called correctly"""
        footage_api_wrapper = Mock()
        footage_id = "dummy_footage_id"
        footage_api_wrapper.request_recorder = Mock(return_value=footage_id)

        sqs_controller = Mock()
        sqs_controller.get_queue_url.return_value = "queue_url"
        message = {
            "Body": Mock()
        }
        sqs_controller.get_message = Mock(return_value=message)
        sqs_controller.delete_message = Mock(return_value=None)

        fixed_now = datetime.now(timezone.utc)

        operator_artifact = MagicMock(spec=OperatorArtifact)
        operator_artifact.event_timestamp = fixed_now - event_timestamp_delta
        operator_artifact.operator_monitoring_start = fixed_now - timedelta(minutes=5)
        operator_artifact.operator_monitoring_end = fixed_now
        operator_artifact.s3_path = None
        operator_artifact.device_id = "DATANAUTS_DEV_01"
        operator_artifact.tenant_id = "datanauts"
        parse_artifact_mock.return_value = operator_artifact

        s3_controller = Mock()
        evaluator = Mock()
        correlator = Mock()
        DBDecision.save_db_decision = Mock()  # type: ignore

        graceful_exit = Mock()
        config = SelectorConfig.model_validate({
            "max_GB_per_device_per_month": 2,
            "total_GB_per_month": 100,
            "upload_window_seconds_start": 300,
            "upload_window_seconds_end": 300
        })
        type(graceful_exit).continue_running = PropertyMock(side_effect=[True, False])

        selector = Selector(
            s3_controller,
            footage_api_wrapper,
            sqs_controller,
            config,
            evaluator,
            correlator
        )

        selector.run(graceful_exit)

        parse_artifact_mock.assert_called_once_with(message["Body"])
        footage_api_wrapper.request_recorder.assert_has_calls(
            [
                call(
                    RecorderType.TRAINING,
                    operator_artifact.device_id,
                    operator_artifact.event_timestamp -
                    timedelta(
                        seconds=config.upload_window_seconds_start),
                    min(fixed_now,
                        operator_artifact.event_timestamp + timedelta(
                            seconds=config.upload_window_seconds_end))),
                call(
                    RecorderType.SNAPSHOT,
                    operator_artifact.device_id,
                    operator_artifact.event_timestamp -
                    timedelta(
                        seconds=config.upload_window_seconds_start),
                    min(fixed_now,
                        operator_artifact.event_timestamp + timedelta(
                            seconds=config.upload_window_seconds_end))),
                call(
                    RecorderType.TRAINING,
                    operator_artifact.device_id,
                    operator_artifact.operator_monitoring_start,
                    operator_artifact.operator_monitoring_end),
                call(
                    RecorderType.SNAPSHOT,
                    operator_artifact.device_id,
                    operator_artifact.operator_monitoring_start,
                    operator_artifact.operator_monitoring_end),
            ])
        assert DBDecision.save_db_decision.call_count == 2
        sqs_controller.delete_message.assert_called_once_with(message)

    @patch("selector.selector.parse_artifact")
    @pytest.mark.parametrize("recorder_type,artifact_type", [
        (RecorderType.FRONT, VideoArtifact),
        (None, SignalsArtifact),
        (None, IMUArtifact),
    ])
    def test_incorrect_srx_artifact(self, parse_artifact_mock: Mock,
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
        correlator = Mock()
        graceful_exit = Mock()
        config = SelectorConfig.load_config_from_yaml_file("config/config.yml")
        type(graceful_exit).continue_running = PropertyMock(side_effect=[True, False])

        selector = Selector(
            s3_controller,
            footage_api_wrapper,
            sqs_controller,
            config,
            evaluator,
            correlator
        )

        selector.run(graceful_exit)

        parse_artifact_mock.assert_called_once_with(message["Body"])
        footage_api_wrapper.request_recorder.assert_not_called()

    @patch("selector.selector.parse_artifact")
    @pytest.mark.parametrize("artifact_type", [
        (SOSOperatorArtifact),
        (PeopleCountOperatorArtifact),
        (CameraBlockedOperatorArtifact),
    ])
    def test_incorrect_sav_artifact(self, parse_artifact_mock: Mock, artifact_type: OperatorArtifact):
        """Ensures that some artifacts do not request footages"""
        footage_api_wrapper = Mock()
        footage_api_wrapper.request_recorder = Mock(side_effect=Exception("What a pretty exception we got here"))

        sqs_controller = Mock()
        sqs_controller.get_queue_url.return_value = "queue_url"
        message = {
            "Body": Mock()
        }
        sqs_controller.get_message = Mock(return_value=message)
        sqs_controller.delete_message = Mock()

        operator_artifact = MagicMock(spec=artifact_type)
        operator_artifact.event_timestamp = datetime.now()
        operator_artifact.device_id = "DATANAUTS_DEV_01"

        parse_artifact_mock.return_value = operator_artifact

        s3_controller = Mock()
        config = Mock()
        evaluator = Mock()
        correlator = Mock()
        graceful_exit = Mock()
        type(graceful_exit).continue_running = PropertyMock(side_effect=[True, False])

        selector = Selector(
            s3_controller,
            footage_api_wrapper,
            sqs_controller,
            config,
            evaluator,
            correlator
        )

        selector.run(graceful_exit)
        parse_artifact_mock.assert_called_once_with(message["Body"])
        sqs_controller.assert_not_called()

    @patch("selector.selector.parse_artifact")
    def test_correlate_video_rules(self, parse_artifact_mock: Mock):
        # GIVEN
        footage_api_wrapper = Mock()

        sqs_controller = Mock()
        sqs_controller.get_queue_url.return_value = "queue_url"
        message = {
            "Body": Mock()
        }
        sqs_controller.get_message = Mock(return_value=message)
        sqs_controller.delete_message = Mock(return_value=None)

        s3_controller = Mock()
        config = Mock()
        evaluator = Mock()
        correlator = Mock()
        correlator.correlate_video_rules = Mock(return_value=True)
        graceful_exit = Mock()
        type(graceful_exit).continue_running = PropertyMock(side_effect=[True, False])

        from_ts = datetime.now(timezone.utc) - timedelta(minutes=5)
        to_ts = datetime.now(timezone.utc)
        video_artifact = MagicMock(spec=S3VideoArtifact)
        video_artifact.footage_id = "foo_footage_id"
        video_artifact.tenant_id = "test_tenant"
        video_artifact.raw_s3_path = "s3://foo-raw-bucket/test_s3_path.mp4"
        video_artifact.timestamp = from_ts
        video_artifact.end_timestamp = to_ts
        video_artifact.artifact_id = "foo_artifact_id"
        video_artifact.recorder = RecorderType.TRAINING
        parse_artifact_mock.return_value = video_artifact

        selector = Selector(
            s3_controller,
            footage_api_wrapper,
            sqs_controller,
            config,
            evaluator,
            correlator
        )

        # WHEN
        selector.run(graceful_exit)

        # THEN
        parse_artifact_mock.assert_called_once_with(message["Body"])
        correlator.correlate_video_rules.assert_called_once_with(video_artifact)
        sqs_controller.delete_message.assert_called_once_with(message)

    @patch("selector.selector.parse_artifact")
    def test_correlate_snapshot_rules(self, parse_artifact_mock: Mock):
        # GIVEN
        footage_api_wrapper = Mock()

        sqs_controller = Mock()
        sqs_controller.get_queue_url.return_value = "queue_url"
        message = {
            "Body": Mock()
        }
        sqs_controller.get_message = Mock(return_value=message)
        sqs_controller.delete_message = Mock(return_value=None)

        s3_controller = Mock()
        config = Mock()
        evaluator = Mock()
        correlator = Mock()
        correlator.correlate_snapshot_rules = Mock(return_value=True)
        graceful_exit = Mock()
        type(graceful_exit).continue_running = PropertyMock(side_effect=[True, False])

        from_ts = datetime.now(timezone.utc) - timedelta(minutes=5)
        to_ts = datetime.now(timezone.utc)
        snap_artifact = MagicMock(spec=SnapshotArtifact)
        snap_artifact.footage_id = "foo_footage_id"
        snap_artifact.tenant_id = "test_tenant"
        snap_artifact.raw_s3_path = "s3://foo-raw-bucket/test_s3_path.jpeg"
        snap_artifact.timestamp = from_ts
        snap_artifact.end_timestamp = to_ts
        snap_artifact.artifact_id = "foo_artifact_id"
        snap_artifact.recorder = RecorderType.TRAINING
        parse_artifact_mock.return_value = snap_artifact

        selector = Selector(
            s3_controller,
            footage_api_wrapper,
            sqs_controller,
            config,
            evaluator,
            correlator
        )

        # WHEN
        selector.run(graceful_exit)

        # THEN
        parse_artifact_mock.assert_called_once_with(message["Body"])
        correlator.correlate_snapshot_rules.assert_called_once_with(snap_artifact)
        sqs_controller.delete_message.assert_called_once_with(message)
