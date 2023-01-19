from datetime import datetime
from typing import Optional
from unittest.mock import MagicMock, Mock, call, patch

import pytest

from healthcheck.config import HealthcheckConfig
from healthcheck.model import (Artifact, ArtifactType, MessageAttributes,
                               SnapshotArtifact, SQSMessage, VideoArtifact)
from healthcheck.worker import HealthCheckWorker
from healthcheck.exceptions import NotYetIngestedError, NotPresentError

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
            s3_dir="test_dir",
            training_whitelist=[
                "test-whitelist-tenant"
            ],
            tenant_blacklist=[
                "test-blacklisted-tenant1",
                "test-blacklisted-tenant2"
            ]
        )

    @pytest.mark.parametrize("input_artifact,expected", [
        (
            VideoArtifact(
                stream_name="stream2_InteriorRecorder",
                device_id="device2",
                tenant_id="tenant2",
                footage_from=datetime.now(),
                footage_to=datetime.now()
            ),
            False
        ),
        (
            VideoArtifact(
                stream_name="stream2_TrainingRecorder",
                device_id="device2",
                tenant_id="tenant2",
                footage_from=datetime.now(),
                footage_to=datetime.now()
            ),
            False
        ),
        (
            VideoArtifact(
                stream_name="stream3_FrontRecorder",
                device_id="device3",
                tenant_id="tenant3",
                footage_from=datetime.now(),
                footage_to=datetime.now()
            ),
            True
        )
    ])
    def test_is_blacklisted_recorder(
            self,
            fix_healthcheck_worker: HealthCheckWorker,
            input_artifact: Artifact,
            expected: bool):
        assert fix_healthcheck_worker.is_blacklisted_recorder(input_artifact) == expected

    @pytest.mark.parametrize("input_artifact,expected_result", [
        (
            VideoArtifact(
                stream_name="stream2_TrainingRecorder",
                device_id="device2",
                tenant_id="test-whitelist-tenant",
                footage_from=datetime.now(),
                footage_to=datetime.now()
            ),
            False
        ),
        (
            VideoArtifact(
                stream_name="stream2_InteriorRecorder",
                device_id="device2",
                tenant_id="test-whitelist-tenant",
                footage_from=datetime.now(),
                footage_to=datetime.now()
            ),
            False
        ),
        (
            VideoArtifact(
                stream_name="stream2_TrainingRecorder",
                device_id="device2",
                tenant_id="tenant2",
                footage_from=datetime.now(),
                footage_to=datetime.now()
            ),
            True
        ),
        (
            SnapshotArtifact(
                    uuid="uuid1",
                    device_id="device1",
                    tenant_id="tenant1",
                    timestamp=datetime.now()
                ),
            False
        ),
        (
            SnapshotArtifact(
                    uuid="uuid1",
                    device_id="device1",
                    tenant_id="test-whitelist-tenant",
                    timestamp=datetime.now()
                ),
            False
        )
    ])
    def test_is_blacklist_training(
            self,
            fix_healthcheck_worker: HealthCheckWorker,
            input_artifact: Artifact,
            expected_result: bool):
        assert fix_healthcheck_worker.is_blacklisted_training(input_artifact) == expected_result


    @pytest.fixture
    def fix_healthcheck_worker(self,
                               fix_config: HealthcheckConfig) -> HealthCheckWorker:
        return HealthCheckWorker(
            config=fix_config,
            graceful_exit=MagicMock(),
            sqs_msg_parser=MagicMock(),
            artifact_msg_parser=MagicMock(),
            sqs_controller=MagicMock(),
            notifier=MagicMock(),
            checkers={
                ArtifactType.INTERIOR_RECORDER: MagicMock(),
                ArtifactType.SNAPSHOT: MagicMock(),
                ArtifactType.TRAINING_RECORDER: MagicMock()
            }
        )

    @pytest.mark.parametrize("input_message, should_blacklist", [
        (
            SQSMessage(
                message_id="test-message",
                receipt_handle="test123",
                timestamp="123455",
                body={
                    "mocked": "body"
                },
                attributes=MessageAttributes(
                    tenant="test-blacklisted-tenant1",
                    device_id="test-device1"
                )
            ),
            True
        ),
        (
            SQSMessage(
                message_id="test-message2",
                receipt_handle="test1234",
                timestamp="123456",
                body={"mocked": "body2"},
                attributes=MessageAttributes(
                    tenant="a-tenant-not-blacklisted",
                    device_id="test-device2"
                )
            ),
            False
        )
    ])
    def test_is_blacklisted_tenant(self,
                                   input_message: SQSMessage,
                                   should_blacklist: bool,
                                   fix_healthcheck_worker: HealthCheckWorker):
        assert fix_healthcheck_worker.is_blacklisted_tenant(input_message) == should_blacklist

    @pytest.mark.parametrize("input_sqs_message,input_artifacts,exception_raised", [
        # empty artifacts
        (
            SQSMessage(
                message_id="mocked_message",
                receipt_handle="mocked_receipt",
                body={},
                timestamp=datetime.now(),
                attributes=MessageAttributes(
                    tenant="tenant1",
                    device_id="device1"
                )
            ),
            [],
            None
        ),
        # snapshot ingestion
        (
            SQSMessage(
                message_id="mocked_message",
                receipt_handle="mocked_receipt",
                body={},
                timestamp=datetime.now(),
                attributes=MessageAttributes(
                    tenant="tenant1",
                    device_id="device1"
                )
            ),
            [
                SnapshotArtifact(
                    uuid="uuid1",
                    device_id="device1",
                    tenant_id="tenant1",
                    timestamp=datetime.now()
                ),
                SnapshotArtifact(
                    uuid="uuid2",
                    device_id="device1",
                    tenant_id="tenant1",
                    timestamp=datetime.now()
                ),
            ],
            None
        ),
        # video ingestion interior recorder
        (
            SQSMessage(
                message_id="mocked_message",
                receipt_handle="mocked_receipt",
                body={},
                timestamp=datetime.now(),
                attributes=MessageAttributes(
                    tenant="tenant2",
                    device_id="device1"
                )
            ),
            [
                VideoArtifact(
                    stream_name="stream2_InteriorRecorder",
                    device_id="device2",
                    tenant_id="tenant2",
                    footage_from=datetime.now(),
                    footage_to=datetime.now()
                )
            ],
            None
        ),
        # blacklisted tenant
        (
            SQSMessage(
                message_id="mocked_message",
                receipt_handle="mocked_receipt",
                body={},
                timestamp=datetime.now(),
                attributes=MessageAttributes(
                    tenant="test-blacklisted-tenant1",
                    device_id="device1"
                )
            ),
            [
                VideoArtifact(
                    stream_name="stream2_InteriorRecorder",
                    device_id="device2",
                    tenant_id="test-blacklisted-tenant1",
                    footage_from=datetime.now(),
                    footage_to=datetime.now()
                )
            ],
            None
        ),
        # blacklisted recorder
        (
            SQSMessage(
                message_id="mocked_message",
                receipt_handle="mocked_receipt",
                body={},
                timestamp=datetime.now(),
                attributes=MessageAttributes(
                    tenant="tenant1",
                    device_id="device1"
                )
            ),
            [
                VideoArtifact(
                    stream_name="stream2_FrontRecorder",
                    device_id="device2",
                    tenant_id="tenant1",
                    footage_from=datetime.now(),
                    footage_to=datetime.now()
                )
            ],
            None
        ),
        (
            SQSMessage(
                message_id="mocked_message",
                receipt_handle="mocked_receipt",
                body={},
                timestamp=datetime.now(),
                attributes=MessageAttributes(
                    tenant="tenant2",
                    device_id="device1"
                )
            ),
            [
                VideoArtifact(
                    stream_name="stream2_InteriorRecorder",
                    device_id="device2",
                    tenant_id="tenant2",
                    footage_from=datetime.now(),
                    footage_to=datetime.now()
                )
            ],
            NotYetIngestedError
        ),
        # Not present error
        (
            SQSMessage(
                message_id="mocked_message",
                receipt_handle="mocked_receipt",
                body={},
                timestamp=datetime.now(),
                attributes=MessageAttributes(
                    tenant="tenant2",
                    device_id="device1"
                )
            ),
            [
                VideoArtifact(
                    stream_name="stream2_InteriorRecorder",
                    device_id="device2",
                    tenant_id="tenant2",
                    footage_from=datetime.now(),
                    footage_to=datetime.now()
                )
            ],
            NotPresentError
        )
    ])
    @patch("healthcheck.worker.time.sleep")
    def test_run(self,
                 sleep_mock: Mock,
                 input_sqs_message: SQSMessage,
                 input_artifacts: list[Artifact],
                 fix_config: HealthcheckConfig,
                 exception_raised: Optional[Exception]):
        queue_url = "my-queue-url"
        raw_sqs_message = "mock-raw-message"

        sqs_controller = Mock()
        sqs_controller.get_queue_url = Mock(return_value=queue_url)
        sqs_controller.get_message = Mock(return_value=raw_sqs_message)
        sqs_controller.delete_message = Mock()
        sqs_controller.increase_visibility_timeout_and_handle_exceptions = Mock()

        sqs_msg_parser = Mock()
        sqs_msg_parser.parse_message = Mock(return_value=input_sqs_message)

        artifact_parser_mock = Mock()
        artifact_parser_mock.parse_message = Mock(return_value=input_artifacts)

        checkers = self.get_checkers(input_artifacts, exception_raised)
        notifier_mock = Mock()

        healthcheck_worker = HealthCheckWorker(
            config=fix_config,
            graceful_exit=MagicMock(),
            artifact_msg_parser=artifact_parser_mock,
            sqs_msg_parser=sqs_msg_parser,
            sqs_controller=sqs_controller,
            notifier=notifier_mock,
            checkers=checkers
        )
        healthcheck_worker.run(Mock(side_effect=[True, False]))
        sqs_controller.get_queue_url.assert_called_once()
        sqs_controller.get_message.assert_called_once_with(queue_url)

        if input_sqs_message.attributes.tenant in fix_config.tenant_blacklist:
            return

        sqs_msg_parser.parse_message.assert_called_once_with(raw_sqs_message)

        if not input_artifacts and len(input_artifacts) == 0:
            return

        if any([recorder in input_artifacts[0].artifact_id for recorder in fix_config.recorder_blacklist]):
            return

        if not exception_raised:
            self.healthcheck_assertions(input_artifacts, checkers)
            sqs_controller.delete_message.assert_called_with(queue_url, input_sqs_message)
        else:
            self.healthcheck_assertions(input_artifacts, checkers)
            if isinstance(exception_raised, NotPresentError):
                notifier_mock.assert_called_once()
            elif isinstance(exception_raised, NotYetIngestedError):
                sqs_controller.increase_visibility_timeout_and_handle_exceptions.assert_called_once_with(
                    queue_url,
                    input_sqs_message
                )


    def get_checkers(self, input_artifacts: list[Artifact], exception_raised) -> dict:
        checkers = {
            ArtifactType.INTERIOR_RECORDER: self.get_mocked_checker(input_artifacts, exception_raised),
            ArtifactType.SNAPSHOT: self.get_mocked_checker(input_artifacts, exception_raised),
            ArtifactType.TRAINING_RECORDER: self.get_mocked_checker(input_artifacts, exception_raised)
        }
        return checkers

    def get_mocked_checker(self, input_artifacts: list[Artifact], exception_raised):
        checker = Mock()
        if exception_raised:
            run_healcheck_mock = Mock(side_effect=exception_raised(input_artifacts[0], "mocked-message"))
        else:
            run_healcheck_mock = Mock()
        checker.run_healthcheck = run_healcheck_mock
        return checker

    def healthcheck_assertions(self,
                               input_artifacts: list[Artifact],
                               checkers: dict[ArtifactType, Mock]) -> None:

        artifact = input_artifacts[0]
        if isinstance(artifact, VideoArtifact):
            checkers[artifact.artifact_type].run_healthcheck.assert_called_once_with(artifact)
        elif isinstance(artifact, SnapshotArtifact):
            checkers[artifact.artifact_type].run_healthcheck.assert_has_calls(
                calls=[call(artifact) for artifact in input_artifacts]
            )
