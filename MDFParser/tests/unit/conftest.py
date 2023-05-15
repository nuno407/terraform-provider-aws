from unittest.mock import ANY, Mock, MagicMock
from typing import Any
import json
import pytest
from mdfparser.consumer import Consumer
from mdfparser.config import MdfParserConfig
from mdfparser.metadata.downloader import MetadataDownloader
from mdfparser.metadata.uploader import MetadataUploader
from mdfparser.interfaces.artifact_adapter import ArtifactAdapter
import os

REGION_NAME = "us-east-1"
CURRENT_LOCATION = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))
SQS_RAW_MESSAGES = os.path.join(
    CURRENT_LOCATION, "test_assets", "sqs_messages")
SQS_PARSED_MESSAGES = os.path.join(
    CURRENT_LOCATION, "test_assets", "sqs_messages", "parsed")


def get_file(path: str) -> bytearray:
    """
    Read raw file.

    Args:
        path (str): path to file.

    Returns:
        bytearray: File content
    """
    with open(path, "rb") as f:
        return f.read()


def get_json_file(dir: str, sqs_message_name: str):
    if not sqs_message_name.endswith(".json"):
        file_name = sqs_message_name + ".json"
    else:
        file_name = sqs_message_name
    file_path = os.path.join(dir, file_name)
    return json.loads(get_file(file_path).decode("utf-8"))


@pytest.fixture
def sqs_parsed_message(request: str) -> dict[Any, Any]:
    return get_json_file(SQS_PARSED_MESSAGES, request.param)


@pytest.fixture
def sqs_raw_message(request: str) -> dict[Any, Any]:
    return get_json_file(SQS_RAW_MESSAGES, request.param)


@pytest.fixture
def container_services_mock() -> MagicMock:
    return MagicMock()


@pytest.fixture
def metadata_handler() -> Mock:
    return Mock()


@pytest.fixture
def imu_handler() -> Mock:
    return Mock()


@pytest.fixture
def s3_client() -> Mock:
    return MagicMock()


@pytest.fixture
def metadata_uploader(container_services_mock, s3_client) -> Mock:
    return MetadataUploader(container_services_mock, s3_client)


@pytest.fixture
def metadata_downloader(container_services_mock, s3_client) -> MetadataDownloader:
    return MetadataDownloader(container_services_mock, s3_client)


@pytest.fixture(scope="function")
def sqs_client() -> MagicMock:
    return MagicMock()


@pytest.fixture()
def config() -> MdfParserConfig:
    """ Return a MdfParserConfig for all tests in this class """
    return MdfParserConfig(
        input_queue="dev-terraform-queue-mdf-parser",
        metadata_output_queue="dev-terraform-queue-metadata",
        temporary_bucket="dev-rcd-temporary-metadata-files"
    )


@pytest.fixture()
def message_adapter() -> ArtifactAdapter:
    return ArtifactAdapter()


@pytest.fixture
def consumer(container_services_mock, metadata_handler, imu_handler, config, sqs_client, message_adapter) -> Consumer:
    return Consumer(container_services_mock, [metadata_handler, imu_handler], config, message_adapter, sqs_client)
