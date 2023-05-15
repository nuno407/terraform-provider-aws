from datetime import datetime
from typing import Optional, Union
from unittest.mock import MagicMock, Mock, PropertyMock, patch

import pytest
from pydantic import parse_obj_as
from pytz import UTC

from base.model.artifacts import (Artifact, RecorderType, SnapshotArtifact,
                                  TimeWindow, VideoArtifact)
from healthcheck.config import HealthcheckConfig
from healthcheck.exceptions import NotPresentError, NotYetIngestedError
from healthcheck.worker import HealthCheckWorker


def image_based_artifact(recorder: RecorderType, tenant_id: str = "tenant1", device_id: str = "device1",
                         **kwargs: dict) -> Union[VideoArtifact, SnapshotArtifact]:
    obj = {
        "tenant_id": tenant_id,
        "device_id": device_id,
        "recorder": recorder,
        "timestamp": datetime.now(tz=UTC),
        "upload_timing": TimeWindow(
            start=datetime.now(tz=UTC),
            end=datetime.now(tz=UTC)),
        **kwargs
    }
    if recorder in [RecorderType.INTERIOR, RecorderType.TRAINING, RecorderType.FRONT]:
        # video required fields
        obj["end_timestamp"] = datetime.now(tz=UTC)
        obj["stream_name"] = "stream1"
    else:
        # snapshot required fields
        obj["uuid"] = "uuid1"

    return parse_obj_as(Union[VideoArtifact, SnapshotArtifact], obj)


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

    @pytest.mark.parametrize("input_artifact,expected_result", [
        (
            image_based_artifact(
                device_id="device2",
                tenant_id="test-whitelist-tenant",
                recorder=RecorderType.TRAINING
            ),
            True
        ),
        (
            image_based_artifact(
                stream_name="stream2_TrainingRecorder",
                device_id="device2",
                tenant_id="tenant2",
                recorder=RecorderType.TRAINING
            ),
            False
        ),
        (
            image_based_artifact(
                recorder=RecorderType.SNAPSHOT
            ),
            True
        ),
        (
            image_based_artifact(
                stream_name="stream4_InteriorRecorder",
                tenant_id="test-whitelist-tenant",
                recorder=RecorderType.INTERIOR
            ),
            True
        )
    ])
    def test_is_whitelisted_training(
            self,
            fix_healthcheck_worker: HealthCheckWorker,
            input_artifact: Artifact,
            expected_result: bool):
        print(fix_healthcheck_worker)
        assert fix_healthcheck_worker.is_relevant(
            input_artifact) == expected_result

    @pytest.fixture
    def fix_healthcheck_worker(self,
                               fix_config: HealthcheckConfig) -> HealthCheckWorker:
        return HealthCheckWorker(
            config=fix_config,
            sqs_controller=MagicMock(),
            notifier=MagicMock(),
            checkers={
                RecorderType.INTERIOR: MagicMock(),
                RecorderType.SNAPSHOT: MagicMock(),
                RecorderType.TRAINING: MagicMock()
            }
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
                stream_name="stream2_InteriorRecorder",
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
                stream_name="stream2_InteriorRecorder",
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
                stream_name="stream2_InteriorRecorder",
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

        checkers = self.get_checkers(input_artifact, exception_raised)
        notifier_mock = Mock()

        healthcheck_worker = HealthCheckWorker(
            config=fix_config,
            sqs_controller=sqs_controller,
            notifier=notifier_mock,
            checkers=checkers
        )
        graceful_exit = MagicMock()
        prop = PropertyMock(side_effect=[True, False])
        type(graceful_exit).continue_running = prop

        healthcheck_worker.run(graceful_exit)
        sqs_controller.get_message.assert_called_once()

        if input_artifact.tenant_id in fix_config.tenant_blacklist:
            return

        if not exception_raised:
            checkers[input_artifact.recorder].run_healthcheck.assert_called_once_with(input_artifact)
            sqs_controller.delete_message.assert_called_with(
                input_sqs_message)
        else:
            checkers[input_artifact.recorder].run_healthcheck.assert_called_once_with(input_artifact)
            if isinstance(exception_raised, NotPresentError):
                notifier_mock.assert_called_once()
            elif isinstance(exception_raised, NotYetIngestedError):
                sqs_controller.try_update_message_visibility_timeout.assert_called_once_with(
                    input_sqs_message
                )

    def get_checkers(self, input_artifact: Artifact, exception_raised) -> dict:
        checkers = {
            RecorderType.INTERIOR: self.get_mocked_checker(input_artifact, exception_raised),
            RecorderType.SNAPSHOT: self.get_mocked_checker(input_artifact, exception_raised),
            RecorderType.TRAINING: self.get_mocked_checker(input_artifact, exception_raised)
        }
        return checkers

    def get_mocked_checker(self, input_artifact: Artifact, exception_raised):
        checker = Mock()
        if exception_raised:
            run_healcheck_mock = Mock(side_effect=exception_raised(
                input_artifact, "mocked-message"))
        else:
            run_healcheck_mock = Mock()
        checker.run_healthcheck = run_healcheck_mock
        return checker
