import json
from datetime import datetime
from typing import Optional, Union
from unittest.mock import MagicMock, Mock, PropertyMock, patch

import pytest
from pydantic import TypeAdapter
from pytz import UTC

from base.model.artifacts import (Artifact, RecorderType, S3VideoArtifact,
                                  SnapshotArtifact, TimeWindow, Recording)
from healthcheck.config import HealthcheckConfig
from healthcheck.exceptions import NotPresentError, NotYetIngestedError
from healthcheck.worker import HealthCheckWorker


def image_based_artifact(recorder: RecorderType, tenant_id: str = "tenant1", device_id: str = "device1",
                         **kwargs: dict) -> Union[S3VideoArtifact, SnapshotArtifact]:
    obj = {
        "artifact_id": "bar",
        "raw_s3_path": "s3://raw/foo/bar.something",
        "anonymized_s3_path": "s3://anonymized/foo/bar.something",
        "tenant_id": tenant_id,
        "device_id": device_id,
        "recorder": recorder,
        "timestamp": datetime.now(tz=UTC),
        "upload_timing": TimeWindow(
            start=datetime.now(tz=UTC),
            end=datetime.now(tz=UTC)),
        "end_timestamp": datetime.now(tz=UTC),
        **kwargs
    }
    if recorder in [RecorderType.INTERIOR, RecorderType.TRAINING, RecorderType.FRONT]:
        # video required fields
        obj["end_timestamp"] = datetime.now(tz=UTC)
        obj["rcc_s3_path"] = "s3://bucket/key"
        obj["footage_id"] = "footage_id1"
        obj["recordings"] = [Recording(chunk_ids=[1, 2, 3], recording_id="recording-id1")]
    else:
        # snapshot required fields
        obj["uuid"] = "uuid1"

    adapter = TypeAdapter(S3VideoArtifact | SnapshotArtifact)

    return adapter.validate_python(obj)


@pytest.mark.unit
class TestWorker:

    @pytest.fixture
    def fix_config(self) -> HealthcheckConfig:
        return HealthcheckConfig(
            input_queue="test-input-queue-1",
            anonymized_s3_bucket="test-anon-bucket1",
            db_name="test-db-name1",
            environment_prefix="test",
            raw_s3_bucket="test-raw-bucket1",
            recorder_blacklist=[
                "FrontRecorder"
            ],
            training_whitelist=[
                "test-whitelist-tenant"
            ],
            tenant_blacklist=[
                "test-blacklisted-tenant1",
                "test-blacklisted-tenant2"
            ]
        )

    @pytest.mark.parametrize("input_sqs_message,input_artifact,exception_raised", [
        # snapshot ingestion
        (
            {
                "Body": "{ \"Message\": { \"attribute\": \"value\" } }"
            },
            image_based_artifact(
                recorder=RecorderType.SNAPSHOT,
            ),
            None
        ),
        # video ingestion interior recorder
        (
            {
                "Body": "{ \"Message\": { \"attribute\": \"value\" } }"
            },
            image_based_artifact(
                device_id="device2",
                tenant_id="tenant2",
                recorder=RecorderType.INTERIOR
            ),
            None
        ),
        (
            {
                "Body": "{ \"Message\": { \"attribute\": \"value\" } }"
            },
            image_based_artifact(
                device_id="device2",
                tenant_id="tenant2",
                recorder=RecorderType.INTERIOR,
            ),
            NotYetIngestedError
        ),
        # Not present error
        (
            {
                "Body": "{ \"Message\": { \"attribute\": \"value\" } }"
            },
            image_based_artifact(
                device_id="device2",
                tenant_id="tenant2",
                recorder=RecorderType.INTERIOR,
            ),
            NotPresentError
        )
    ])
    @patch("healthcheck.worker.parse_artifact")
    def test_run(self,
                 patched_artifact_parse_message: Mock,
                 input_sqs_message: dict,
                 input_artifact: Artifact,
                 fix_config: HealthcheckConfig,
                 exception_raised: Optional[Exception]):
        sqs_controller = Mock()
        sqs_controller.get_message = Mock(return_value=input_sqs_message)
        sqs_controller.delete_message = Mock()
        sqs_controller.try_update_message_visibility_timeout = Mock()

        patched_artifact_parse_message.return_value = input_artifact
        checker = Mock()

        check_determiner = Mock()
        check_determiner.get_checker = Mock(return_value=checker)

        if exception_raised is not None:
            checker.get_checker = Mock(side_effect=exception_raised)
        else:
            checker.get_checker = Mock()

        notifier_mock = Mock()

        healthcheck_worker = HealthCheckWorker(
            sqs_controller=sqs_controller,
            notifier=notifier_mock,
            checker_determiner=check_determiner
        )
        graceful_exit = MagicMock()
        prop = PropertyMock(side_effect=[True, False])
        type(graceful_exit).continue_running = prop

        healthcheck_worker.run(graceful_exit)
        sqs_controller.get_message.assert_called_once()

        check_determiner.get_checker.assert_called_once_with(input_artifact)

        if not exception_raised:
            checker.run_healthcheck.assert_called_once_with(input_artifact)
            sqs_controller.delete_message.assert_called_with(
                input_sqs_message)
        else:
            checker.run_healthcheck.assert_called_once_with(input_artifact)
            if isinstance(exception_raised, NotPresentError):
                notifier_mock.assert_called_once()
                sqs_controller.delete_message.assert_called_once()
            elif isinstance(exception_raised, NotYetIngestedError):
                sqs_controller.try_update_message_visibility_timeout.assert_called_once_with(
                    input_sqs_message
                )
                sqs_controller.delete_message.assert_not_called()
