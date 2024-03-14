import json
import pytest
from base64 import b64decode, b64encode
import io
import pandas as pd
import requests_mock
from artifact_downloader.memory_buffer import UnclosableMemoryBuffer
from typing import Callable, Optional
from unittest.mock import Mock

from mypy_boto3_sqs import SQSClient

from base.testing.utils import assert_parquet_streams
from base.testing.s3_state_manager import S3StateLoader
from base.aws.sqs import SQSController
from utils import load_relative_post_data, load_relative_sqs_message, transform_imu_json_bytes_to_parquet
from base.model.artifacts import parse_all_models
from artifact_downloader.config import ArtifactDownloaderConfig


class TestAPIDownloader:
    @pytest.fixture
    def imu_endpoint(self, config: ArtifactDownloaderConfig) -> str:
        return str(config.artifact_base_url) + "ridecare/imu/video"

    @pytest.mark.integration
    def test_imu_success(self,
                         moto_sqs_client: SQSClient,
                         s3_state_loader: S3StateLoader,
                         input_queue_controller: SQSController,
                         imu_endpoint: str,
                         main_function: Callable,
                         mock_requests: requests_mock.Mocker):
        """
        Tests IMU end2end

        REMARKS:
        For errors related to "botocore.errorfactory.NoSuchBucket" make sure that the bucket name used
        in the sqs message and S3 state file is the same as the one used in the test message.
        """

        # GIVEN
        # Load data
        s3_state_filename = "imu_processing_s3_state.json"
        post_artifact_data_filename = "imu_processing_post_data.json"
        input_sqs_message_filename = "imu_processing_sqs_message.json"
        sqs_message = load_relative_sqs_message(input_sqs_message_filename)
        sqs_message_dict = load_relative_sqs_message(input_sqs_message_filename)
        sqs_message_dict["Body"] = json.loads(sqs_message_dict["Body"])

        s3_state_loader.load_s3_state(s3_state_filename)
        post_data = load_relative_post_data(post_artifact_data_filename)

        # Load parquet imu data
        imu_data_bytes = s3_state_loader.get_s3_file_content(sqs_message_dict["Body"]["parsed_file_path"])
        imu_data_parquet_buffer = transform_imu_json_bytes_to_parquet(imu_data_bytes)

        # Patch expected data with the converted parquet data
        model_expected = parse_all_models(post_data).model_dump()
        model_expected["data"] = b64encode(imu_data_parquet_buffer.getvalue()).decode("utf-8")

        # Setup request mock
        success_adapter = mock_requests.post(url=imu_endpoint, status_code=200)

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

        # Dump api request
        model_result = parse_all_models(success_adapter.last_request.json()).model_dump()

        # Assert IMU stream
        assert_parquet_streams(
            io.BytesIO(b64decode(model_result["data"])),
            io.BytesIO(b64decode(model_expected["data"]))
        )

        # Remove data, because of floating point mismatch
        del model_result["data"]
        del model_expected["data"]

        # Assert the rest of the model
        assert success_adapter.called_once
        assert model_result == model_expected
