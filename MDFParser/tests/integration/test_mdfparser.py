"""Integration test module for interior recorder."""
from pytest_lazyfixture import lazy_fixture
import pytest
from typing import Any, Optional
from mypy_boto3_s3 import S3Client
from mypy_boto3_sqs import SQSClient
from mdfparser.consumer import Consumer
from unittest.mock import Mock
from base.aws.container_services import ContainerServices
import json
import re
from mdfparser.bootstrap import bootstrap_di
import os
CURRENT_LOCATION = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))
S3_DATA = os.path.join(CURRENT_LOCATION, "data", "s3_data")
SQS_MESSAGES = os.path.join(CURRENT_LOCATION, "data", "sqs_messages")


def helper_split_s3_path(s3_path: str) -> tuple[str, str, str]:
    match = re.match(r"^s3://([^/]+)/(.*)$", s3_path)
    bucket = match.group(1)
    key = match.group(2)

    file_name = key.split("/")[-1]

    return bucket, key, file_name


def helper_download_file_from_bucket(s3_client: S3Client, s3_path: str) -> bytes:
    bucket, key, _ = helper_split_s3_path(s3_path)
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read()


def local_file(path: str) -> bytes:
    local_file_path = os.path.join(S3_DATA, path)
    with open(local_file_path, "rb") as f:
        return f.read()


def local_sqs_message(path: str) -> json:
    local_file_path = os.path.join(SQS_MESSAGES, path)
    with open(local_file_path, "r") as f:
        return json.load(f)


def helper_msg_parser(msg_body: str) -> dict[Any, Any]:
    return json.loads(msg_body.replace("\\", "").replace("\n", "").replace("'", ""))


def helper_to_json(file: bytes) -> dict[Any, Any]:
    return json.loads(file)


@pytest.mark.usefixtures("input_sqs_message", "output_sqs_metadata", "consumer_mocks",
                         "dev_input_queue_url", "dev_output_queue_url")
class TestMDFParser:
    @ pytest.mark.integration
    @ pytest.mark.parametrize("input_sqs_message, output_sqs_metadata, expected_output_file, input_file", [
        # Test Metadata
        (
            local_sqs_message("mdf_queue_metadata.json"),
            local_sqs_message("metadata_queue_metadata.json"),
            local_file("datanauts_DATANAUTS_DEV_02_InteriorRecorder_1680540223210_1680540250651_signals.json"),
            local_file("datanauts_DATANAUTS_DEV_02_InteriorRecorder_1680540223210_1680540250651_metadata_full.json"),
        ),
        # Test IMU
        (
            local_sqs_message("mdf_queue_imu.json"),
            local_sqs_message("metadata_queue_imu.json"),
            local_file("datanauts_DATANAUTS_DEV_02_TrainingRecorder_1680541729312_1680541745612_processed_imu.json"),
            local_file("datanauts_DATANAUTS_DEV_02_TrainingRecorder_1680541729312_1680541745612_imu.csv")
        ),
        # Test IMU V2
        (
            local_sqs_message("mdf_queue_imu_v2.json"),
            local_sqs_message("metadata_queue_imu_v2.json"),
            local_file("datanauts_IMU_V2_TrainingRecorder_1680541729312_1680541745613_processed_imu.json"),
            local_file("datanauts_IMU_V2_TrainingRecorder_1680541729312_1680541745613_imu.csv")
        )
    ], ids=["metadata_integration_test_1", "imu_integration_test_v1", "imu_integration_test_v2"])
    def test_mdfparser_success(self,
                               input_sqs_message: dict[Any,
                                                       Any],
                               output_sqs_metadata: Optional[dict[Any,
                                                                  Any]],
                               expected_output_file: bytes,
                               input_file: bytes,
                               consumer_mocks: tuple[Consumer, SQSClient, S3Client],
                               dev_input_queue_url: str,
                               dev_output_queue_url: str):
        """
        This test function mocks the SQS and S3 and tests the component end2end.

        Args:
            input_sqs_message (dict[Any, Any]): _description_
            output_sqs_metadata (Optional[dict[Any, Any]]): _description_
            expected_output_file (bytes): _description_
            input_file (bytes): _description_
            consumer_mocks (tuple[Consumer, SQSClient, S3Client]): _description_
            dev_input_queue_url (str): _description_
            dev_output_queue_url (str): _description_
        """

        consumer, moto_sqs_client, moto_s3_client = consumer_mocks

        # Load sqs messages to memory
        input_file_path = input_sqs_message["s3_path"]
        output_file = output_sqs_metadata["parsed_file_path"]
        json_input_message = json.dumps(input_sqs_message)

        # Creates a bucket and uploads the file based on the message
        input_bucket, input_key, _ = helper_split_s3_path(input_file_path)
        moto_s3_client.put_object(Key=input_key, Bucket=input_bucket, Body=input_file)

        # Insert message
        moto_sqs_client.send_message(QueueUrl=dev_input_queue_url, MessageBody=json_input_message)

        # Run
        consumer.run(Mock(side_effect=[True, False]))

        metadata_msg = moto_sqs_client.receive_message(QueueUrl=dev_output_queue_url, WaitTimeSeconds=2)
        mdfparser_msg = moto_sqs_client.receive_message(QueueUrl=dev_input_queue_url, WaitTimeSeconds=2)
        output_file_data = helper_download_file_from_bucket(moto_s3_client, output_file)

        # Assert
        json_metadata_msg = helper_msg_parser(metadata_msg["Messages"][0]["Body"])
        assert json_metadata_msg == output_sqs_metadata

        assert helper_to_json(expected_output_file) == helper_to_json(output_file_data)
        assert "Messages" not in mdfparser_msg
