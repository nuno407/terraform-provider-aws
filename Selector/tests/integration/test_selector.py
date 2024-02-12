
""" Integration test. """

import pytest
from datetime import datetime, timezone
from unittest.mock import Mock, PropertyMock
from typing import Any, Callable
from mypy_boto3_s3 import S3Client
from mypy_boto3_sqs import SQSClient
from base.graceful_exit import GracefulExit
from unittest.mock import Mock, call
from base.model.artifacts import parse_artifact, parse_upload_rule, S3VideoArtifact, VideoUploadRule
from base.aws.s3 import S3Controller
from base.aws.sqs import SQSController
from .helper_functions import get_sqs_message, get_s3_file
from selector.footage_api_wrapper import FootageApiWrapper
from base.model.artifacts import RecorderType
from selector.decision import Decision
import json

# pylint: disable=missing-class-docstring,missing-function-docstring,too-few-public-methods


class TestSelector:

    @ pytest.mark.integration
    @ pytest.mark.parametrize("sqs_message, s3_file, decision_list", [
        # Test camera view blocked
        (
            get_sqs_message("sqs_message_preview_real_cvb.json"),
            get_s3_file("preview_metadata_real_cvb.json"),
            [
                Decision(
                    "Camera completely blocked",
                    "1.0.0",
                    RecorderType.TRAINING,
                    datetime(2023, 7, 3, 8, 37, 59, 467000, tzinfo=timezone.utc),
                    datetime(2023, 7, 3, 8, 38, 29, 461000, tzinfo=timezone.utc)
                )
            ]
        ),
        # Test camera view blocked
        (
            get_sqs_message("sqs_message_preview_cvb_2.json"),
            get_s3_file("preview_metadata_cvb_2.json"),
            [
                Decision(
                    "Camera completely blocked",
                    "1.0.0",
                    RecorderType.TRAINING,
                    datetime(2023, 7, 5, 14, 8, 54, 177000, tzinfo=timezone.utc),
                    datetime(2023, 7, 5, 14, 9, 24, 123000, tzinfo=timezone.utc)
                )
            ]
        ),
        # Test camera view blocked and out of ride boundaries
        (
            get_sqs_message("sqs_message_preview_real_cvb.json"),
            get_s3_file("preview_metadata_cvb_overtime.json"),
            [
                Decision(
                    "Camera completely blocked",
                    "1.0.0",
                    RecorderType.TRAINING,
                    datetime(2023, 7, 3, 8, 37, 59, 467000, tzinfo=timezone.utc),
                    datetime(2023, 7, 3, 8, 38, 29, 461000, tzinfo=timezone.utc)
                )
            ]
        ),
        # Test no training request
        (
            get_sqs_message("sqs_message_preview_real_cvb.json"),
            get_s3_file("preview_metadata_no_upload.json"),
            [
            ]
        ),
        # Test no preview frames
        (
            get_sqs_message("sqs_message_preview_without_frame.json"),
            get_s3_file("preview_metadata_without_frames.json"),
            [
            ]
        ),
        # Test interior recorder legacy request
        (
            get_sqs_message("sqs_message_interior.json"),
            b'random_bytes',
            [
                Decision(
                    "Camera completely blocked",
                    "1.0.0",
                    RecorderType.TRAINING,
                    datetime(2023, 6, 29, 17, 32, 51, 543226, tzinfo=timezone.utc),
                    datetime(2023, 6, 29, 17, 33, 51, 543226, tzinfo=timezone.utc)
                )
            ]
        )

    ], ids=["selector_integration_test_1", "selector_integration_test_2", "selector_integration_test_3", "selector_integration_test_4", "selector_integration_test_5", "selector_integration_test_6"], scope="function")
    def test_selector(self,
                      sqs_message: dict[Any, Any],
                      s3_file: bytes,
                      dev_input_bucket: str,
                      dev_input_queue_name: str,
                      moto_s3_client: S3Client,
                      moto_sqs_client: SQSClient,
                      main_function: Callable,
                      footage_api: FootageApiWrapper,
                      decision_list: list[Decision],
                      graceful_exit: GracefulExit):
        """
        This test function mocks the SQS and S3 and tests the component end2end.

        Remarks: The API is mocked directly in the FootageAPIWrapper


        Args:
            sqs_message (dict[Any, Any]): _description_
            s3_file (bytes): _description_
            dev_input_bucket (str): _description_
            dev_input_queue_name (str): _description_
            moto_s3_client (S3Client): _description_
            moto_sqs_client (SQSClient): _description_
            main_function (Callable): _description_
            footage_api (FootageApiWrapper): _description_
            decision_list (list[Decision]): _description_
            one_time_gracefull_exit (GracefulExit): _description_
        """
        # GIVEN
        type(graceful_exit).continue_running = PropertyMock(
            side_effect=[True, False])

        artifact = parse_artifact(sqs_message)
        _, key = S3Controller.get_s3_path_parts(artifact.s3_path)

        moto_s3_client.put_object(Bucket=dev_input_bucket, Key=key, Body=s3_file)
        queue_answer = moto_sqs_client.get_queue_url(QueueName=dev_input_queue_name)
        moto_sqs_client.send_message(
            QueueUrl=queue_answer["QueueUrl"],
            MessageBody=json.dumps(sqs_message))
        footage_api.request_recorder = Mock()  # type: ignore

        expected_calls = [
            call(
                decision.recorder,
                artifact.device_id,
                decision.footage_from,
                decision.footage_to) for decision in decision_list]

        # WHEN
        main_function()

        # THEN
        if len(decision_list) == 0:
            footage_api.request_recorder.assert_not_called()
            return

        # Ensure no call goes over the limits of the artifact
        for call_obj in footage_api.request_recorder.call_args_list:
            _, _, footage_from, footage_to = call_obj.args
            assert footage_from >= artifact.timestamp and footage_to <= artifact.end_timestamp

        footage_api.request_recorder.assert_has_calls(expected_calls, any_order=True)

    @ pytest.mark.integration
    @ pytest.mark.parametrize("sqs_message_preview, sqs_message_training, sqs_metadata_message, s3_file, decision", [
        # Test camera view blocked
        (
            get_sqs_message("sqs_message_preview_camera_blocked_correlatable.json"),
            get_sqs_message("sqs_message_training_camera_blocked_correlatable.json"),
            get_sqs_message("sqs_message_metadata_rules_camera_blocked_correlatable.json"),
            get_s3_file("preview_metadata_camera_blocked_correlatable.json"),
            Decision(
                "Camera completely blocked",
                "1.0.0",
                RecorderType.TRAINING,
                datetime(2023, 12, 13, 15, 26, 48, 503000, tzinfo=timezone.utc),
                datetime(2023, 12, 13, 15, 28, 19, 528000, tzinfo=timezone.utc)
            )
        )
    ], ids=["test_video_correlation"])
    def test_selector_correlation(self,
                                  sqs_message_preview: dict[Any, Any],
                                  sqs_message_training: dict[Any, Any],
                                  sqs_metadata_message: dict[Any, Any],
                                  s3_file: bytes,
                                  dev_input_bucket: str,
                                  dev_input_queue_name: str,
                                  moto_s3_client: S3Client,
                                  moto_sqs_client: SQSClient,
                                  main_function: Callable,
                                  footage_api: FootageApiWrapper,
                                  decision: Decision,
                                  dev_metadata_controller: SQSController,
                                  graceful_exit: GracefulExit):
        """Test the ingestion of PreviewMetadata, its consequent correlation with an S3Video, and
           the forwarding of a rule to Metadata service.
           Mocks the footage_api, a MongoDB instance, and the selector and metadata queues.

        Args:
            sqs_message_preview (dict[Any, Any]): _description_
            sqs_message_training (dict[Any, Any]): _description_
            sqs_metadata_message (dict[Any, Any]): _description_
            s3_file (bytes): _description_
            dev_input_bucket (str): _description_
            dev_input_queue_name (str): _description_
            moto_s3_client (S3Client): _description_
            moto_sqs_client (SQSClient): _description_
            main_function (Callable): _description_
            footage_api (FootageApiWrapper): _description_
            decision (Decision): _description_
            dev_metadata_controller (SQSController): _description_
            graceful_exit (GracefulExit): _description_
        """
        # GIVEN
        type(graceful_exit).continue_running = PropertyMock(
            side_effect=[True, True, False])

        preview_artifact = parse_artifact(sqs_message_preview)
        _, key = S3Controller.get_s3_path_parts(preview_artifact.s3_path)

        training_artifact: S3VideoArtifact = parse_artifact(sqs_message_training)
        exepected_metadata_artifact: VideoUploadRule = parse_upload_rule(sqs_metadata_message)
        moto_s3_client.put_object(Bucket=dev_input_bucket, Key=key, Body=s3_file)
        queue_answer = moto_sqs_client.get_queue_url(QueueName=dev_input_queue_name)
        moto_sqs_client.send_message(
            QueueUrl=queue_answer["QueueUrl"],
            MessageBody=json.dumps(sqs_message_preview))
        moto_sqs_client.send_message(
            QueueUrl=queue_answer["QueueUrl"],
            MessageBody=json.dumps(sqs_message_training))
        footage_api.request_recorder = Mock(return_value=training_artifact.footage_id)  # type: ignore

        # WHEN
        main_function()

        # THEN

        # Ensure no call goes over the limits of the artifact
        for call_obj in footage_api.request_recorder.call_args_list:
            _, _, footage_from, footage_to = call_obj.args
            assert footage_from >= preview_artifact.timestamp and footage_to <= preview_artifact.end_timestamp

        footage_api.request_recorder.assert_called_once_with(
            decision.recorder,
            preview_artifact.device_id,
            decision.footage_from,
            decision.footage_to
        )
        metadata_artifact = parse_upload_rule(dev_metadata_controller.get_message()["Body"])
        assert metadata_artifact == exepected_metadata_artifact
