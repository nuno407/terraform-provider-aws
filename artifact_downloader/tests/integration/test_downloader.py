import pytest
import requests_mock
from typing import Callable, Optional
from unittest.mock import Mock

from mypy_boto3_sqs import SQSClient

from base.testing.s3_state_manager import S3StateLoader
from base.aws.sqs import SQSController
from utils import load_relative_post_data, load_relative_sqs_message
from base.model.artifacts import parse_all_models

# autopep8: off
class TestAPIDownloader:
    @pytest.mark.integration
    @pytest.mark.parametrize("input_sqs_message_filename, s3_state_filename, endpoint, post_artifact_data_filename",
        [
            (
                "snapshot_sqs_message.json",
                "snapshot_s3_state.json",
                "ridecare/snapshots",
                "snapshot_artifact_post_data.json"
            ),
            (
                "video_sqs_message.json",
                "video_s3_state.json",
                "ridecare/video",
                "video_artifact_post_data.json"
            ),
            (
                "device_info_event_sqs_message.json",
                None,
                "ridecare/event",
                "device_info_event_post_data.json"
            ),
            (
                "device_incident_event_sqs_message.json",
                None,
                "ridecare/event",
                "device_incident_event_post_data.json"
            ),
            (
                "upload_rule_snapshot_message.json",
                None,
                "ridecare/upload_rule/snapshot",
                "upload_rule_snapshot_post_data.json"
            ),
            (
                "upload_rule_video_message.json",
                None,
                "ridecare/upload_rule/video",
                "upload_rule_video_post_data.json"
            ),
            (
                "sanitizer_camera_blocked_operator_artifact_sqs_message.json",
                None,
                "ridecare/operator",
                "sanitizer_camera_blocked_operator_artifact_post_data.json"
            ),
            (
                "sanitizer_people_count_operator_artifact_sqs_message.json",
                None,
                "ridecare/operator",
                "sanitizer_people_count_operator_artifact_post_data.json"
            ),
            (
                "sanitizer_sos_operator_artifact_sqs_message.json",
                None,
                "ridecare/operator",
                "sanitizer_sos_operator_artifact_post_data.json"
            ),
            (
                "training_snapshot_metadata_sqs_message.json",
                "training_snapshot_metadata_s3_state.json",
                "ridecare/signals/snapshot",
                "training_snapshot_metadata_post_data.json"
            ),
            (
                "sdm_message.json",
                None,
                "ridecare/pipeline/status",
                "sdm_message_post_data.json"
            ),
            (
                "mdfparser_message.json",
                "mdfparser_state.json",
                "ridecare/signals/video",
                "mdfparser_post_data.json",
            ),
            (
                "anonymization_image_result_sqs_message.json",
                "anonymized_s3_state.json",
                "ridecare/pipeline/anonymize/snapshot",
                "anonymization_snap_result_post_data.json",
            ),
            (
                "anonymization_video_result_sqs_message.json",
                "anonymized_s3_state.json",
                "ridecare/pipeline/anonymize/video",
                "anonymization_video_result_post_data.json",
            ),
            (
                "chc_video_result_sqs_message.json",
                "chc_s3_state.json",
                "ridecare/pipeline/chc/video",
                "chc_video_result_post_data.json",
            )
        ],
        ids=["snapshot_test_success",
            "video_test_success",
            "device_info_event_success",
            "device_incident_event_success",
            "rule_snapshot_test_success",
            "rule_video_test_success",
            "sanitizer_camera_blocked_operator_artifact",
            "sanitizer_people_count_operator_artifact",
            "sanitizer_sos_operator_artifact",
            "training_snapshot_metadata_test_success",
            "sdm_message_test_success",
            "mdfparser_test_success",
            "anonymize_snapshot_test",
            "anonymize_video_test",
            "chc_video_test"
        ],
        indirect=["endpoint"])
    # autopep8: on
    def test_component_success(self,
                               moto_sqs_client: SQSClient,
                               s3_state_loader: S3StateLoader,
                               input_queue_controller: SQSController,
                               input_sqs_message_filename: str,
                               s3_state_filename: Optional[str],
                               endpoint: str,
                               main_function: Callable,
                               post_artifact_data_filename: str,
                               mock_requests: requests_mock.Mocker):
        """
        Test for the component end-to-end. To add a new test 4 arguments needs to be specified:
        - input_sqs_message_filename:
            The file name containing a message to be loaded into the input sqs,
            this file needs to be placed under data/sqs_messages
        - s3_state_filename:
            The file name containing an S3 state to be used by the loader S3StateLoader (more info
            under the definition of the file), if the artifact does not use S3 this can be set to None.
            Otherwise this file needs to be placed under data/s3_cloud_state.
        - endpoint:
            The endpoint suffix used for the post on the API.
        - post_artifact_data_filename:
            A file containing the json data that is posted by the artifact_downloader. This file needs to be placed
            under data/post_data.

        REMARKS:
        For errors related to "botocore.errorfactory.NoSuchBucket" make sure that the bucket name used
        in the sqs message and S3 state file is the same as the one used in the test message.

        Make sure that ffprobe is installed in the system, this can be installed by running:
        sudo apt-get install ffmpeg
        """

        # GIVEN
        # Load data
        sqs_message = load_relative_sqs_message(input_sqs_message_filename)

        if s3_state_filename is not None:
            s3_state_loader.load_s3_state(s3_state_filename)
        post_data = load_relative_post_data(post_artifact_data_filename)
        post_data_model = parse_all_models(post_data)

        # Setup request mock
        success_adapter = mock_requests.post(url=endpoint, status_code=200)

        # Setup SQS
        input_queue_controller.delete_message = Mock()
        queue_url = input_queue_controller.get_queue_url()
        moto_sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=sqs_message.get("Body"),
            MessageAttributes=sqs_message.get(
                "MessageAttributes",
                {}))

        # WHEN
        main_function()
        # THEN
        input_queue_controller.delete_message.assert_called_once()
        # Assert mock requests
        assert success_adapter.called_once
        assert parse_all_models(success_adapter.last_request.json()).model_dump() == post_data_model.model_dump()

    # autopep8: off
    @pytest.mark.integration
    @pytest.mark.parametrize("input_sqs_message_filename, s3_state_filename, endpoint, post_artifact_data_filename", [
            (
                "snapshot_sqs_message.json",
                "snapshot_s3_state.json",
                "ridecare/snapshots",
                "snapshot_artifact_post_data.json"
            ),
            (
                "video_sqs_message.json",
                "video_s3_state.json",
                "ridecare/video",
                "video_artifact_post_data.json"
            ),
            (
                "device_info_event_sqs_message.json",
                None,
                "ridecare/event",
                "device_info_event_post_data.json"
            ),
            (
                "device_incident_event_sqs_message.json",
                None,
                "ridecare/event",
                "device_incident_event_post_data.json"
            ),
            (
                "upload_rule_snapshot_message.json",
                None,
                "ridecare/upload_rule/snapshot",
                "upload_rule_snapshot_post_data.json"
            ),
            (
                "upload_rule_video_message.json",
                None,
                "ridecare/upload_rule/video",
                "upload_rule_video_post_data.json"
            ),
            (
                "sanitizer_camera_blocked_operator_artifact_sqs_message.json",
                None,
                "ridecare/operator",
                "sanitizer_camera_blocked_operator_artifact_post_data.json"
            ),
            (
                "sanitizer_people_count_operator_artifact_sqs_message.json",
                None,
                "ridecare/operator",
                "sanitizer_people_count_operator_artifact_post_data.json"
            ),
            (
                "sanitizer_sos_operator_artifact_sqs_message.json",
                None,
                "ridecare/operator",
                "sanitizer_sos_operator_artifact_post_data.json"
            ),
            (
                "sdm_message.json",
                None,
                "ridecare/pipeline/status",
                "sdm_message_post_data.json",
            ),
            (
                "mdfparser_message.json",
                "mdfparser_state.json",
                "ridecare/signals/video",
                "mdfparser_post_data.json",
            ),
            (
                "anonymization_image_result_sqs_message.json",
                "anonymized_s3_state.json",
                "ridecare/pipeline/anonymize/snapshot",
                "anonymization_snap_result_post_data.json",
            ),
            (
                "anonymization_video_result_sqs_message.json",
                "anonymized_s3_state.json",
                "ridecare/pipeline/anonymize/video",
                "anonymization_video_result_post_data.json",
            ),
            (
                "chc_video_result_sqs_message.json",
                "chc_s3_state.json",
                "ridecare/pipeline/chc/video",
                "chc_video_result_post_data.json",
            )
            ],
            ids=["snapshot_test_failure",
                "video_test_failure",
                "device_info_event_failure",
                "device_incident_event_failure",
                "rule_snapshot_test_failure",
                "rule_video_test_failure",
                "sanitizer_camera_blocked_operator_artifact",
                "sanitizer_people_count_operator_artifact",
                "sanitizer_sos_operator_artifact",
                "sdm_message_test_failure",
                "mdfparser_test_failure",
                "anonymize_snapshot_test",
                "anonymize_video_test",
                "chc_video_test"
            ],
            indirect=["endpoint"])
    # autopep8: on
    def test_component_failure(self,
                               moto_sqs_client: SQSClient,
                               s3_state_loader: S3StateLoader,
                               input_queue_controller: SQSController,
                               input_sqs_message_filename: str,
                               s3_state_filename: Optional[str],
                               endpoint: str,
                               main_function: Callable,
                               post_artifact_data_filename: str,
                               mock_requests: requests_mock.Mocker):
        """
        Test for the component end-to-end. To add a new test 4 arguments needs to be specified:
        - input_sqs_message_filename:
            The file name containing a message to be loaded into the input sqs,
            this file needs to be placed under data/sqs_messages
        - s3_state_filename:
            The file name containing an S3 state to be used by the loader S3StateLoader (more info
            under the definition of the file), if the artifact does not use S3 this can be set to None.
            Otherwise this file needs to be placed under data/s3_cloud_state.
        - endpoint:
            The endpoint suffix used for the post on the API.
        - post_artifact_data_filename:
            A file containing the json data that is posted by the artifact_downloader. This file needs to be placed
            under data/post_data.

        REMARKS:
        For errors related to "botocore.errorfactory.NoSuchBucket" make sure that the bucket name used
        in the sqs message and S3 state file is the same as the one used in the test message.

        Make sure that ffprobe is installed in the system, this can be installed by running:
        sudo apt-get install ffmpeg
        """

        # GIVEN
        # Load data
        sqs_message = load_relative_sqs_message(input_sqs_message_filename)

        if s3_state_filename is not None:
            s3_state_loader.load_s3_state(s3_state_filename)
        post_data = load_relative_post_data(post_artifact_data_filename)
        post_data_model = parse_all_models(post_data)

        # Setup request mock
        link_adapter = mock_requests.post(url=endpoint, status_code=422)

        # Setup SQS
        input_queue_controller.delete_message = Mock()
        queue_url = input_queue_controller.get_queue_url()
        print(sqs_message.get("Body"))
        moto_sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=sqs_message.get("Body"),
            MessageAttributes=sqs_message.get(
                "MessageAttributes",
                {}))

        # WHEN
        main_function()

        # THEN
        input_queue_controller.delete_message.assert_not_called()
        # Assert mock requests
        assert link_adapter.called_once
        assert parse_all_models(link_adapter.last_request.json()) == post_data_model
