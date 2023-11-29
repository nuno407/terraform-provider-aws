import pytest
import requests_mock
from typing import Callable, Optional
from unittest.mock import Mock

from mypy_boto3_sqs import SQSClient

from base.testing.s3_state_manager import S3StateLoader
from base.aws.sqs import SQSController
from utils import load_relative_post_data, load_relative_sqs_message


class TestAPIDownloader:
    @pytest.mark.integration
    @pytest.mark.parametrize("input_sqs_message_filename, s3_state_filename, endpoint, post_artifact_data_filename", [
        (
            "snapshot_sqs_message.json",
            "snapshot_s3_state.json",
            "ridecare/snapshots",
            "snapshot_artifact_post_data.json"
        )
    ], ids=["snapshot_test_success"], indirect=["endpoint"])
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
        """

        # GIVEN
        # Load data
        sqs_message = load_relative_sqs_message(input_sqs_message_filename)

        if s3_state_filename is not None:
            s3_state_loader.load_s3_state(s3_state_filename)
        post_data = load_relative_post_data(post_artifact_data_filename)

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
        assert success_adapter.last_request.json() == post_data

    @pytest.mark.integration
    @pytest.mark.parametrize("input_sqs_message_filename, s3_state_filename, endpoint, post_artifact_data_filename", [
        (
            "snapshot_sqs_message.json",
            "snapshot_s3_state.json",
            "ridecare/snapshots",
            "snapshot_artifact_post_data.json"
        )
    ], ids=["snapshot_test_failure"], indirect=["endpoint"])
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
        """

        # GIVEN
        # Load data
        sqs_message = load_relative_sqs_message(input_sqs_message_filename)

        if s3_state_filename is not None:
            s3_state_loader.load_s3_state(s3_state_filename)
        post_data = load_relative_post_data(post_artifact_data_filename)

        # Setup request mock
        link_adapter = mock_requests.post(url=endpoint, status_code=422)

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
        input_queue_controller.delete_message.assert_not_called()
        # Assert mock requests
        assert link_adapter.called_once
        assert link_adapter.last_request.json() == post_data
