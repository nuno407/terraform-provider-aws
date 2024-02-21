import os
import pytest
import requests_mock
from moto import mock_s3, mock_sqs
import boto3
from unittest.mock import Mock, PropertyMock, patch
from mypy_boto3_s3 import S3Client
from mypy_boto3_sqs import SQSClient
from base.testing.s3_state_manager import S3StateLoader
from base.testing.utils import get_abs_path
from base.aws.sqs import SQSController
from artifact_downloader.main import main
from artifact_downloader.config import ArtifactDownloaderConfig
from artifact_downloader.bootstrap import bootstrap_di
from kink import di
from base.graceful_exit import GracefulExit
from base.testing.mock_functions import set_mock_aws_credentials

set_mock_aws_credentials()
AWS_REGION = "us-east-1"


@pytest.fixture
def config_path() -> str:
    return get_abs_path(__file__, "data/config.yml")


@pytest.fixture
def config(config_path: str) -> ArtifactDownloaderConfig:
    return ArtifactDownloaderConfig.load_yaml_config(config_path)


@pytest.fixture
def anon_bucket(config: ArtifactDownloaderConfig) -> str:
    return config.raw_bucket.replace("raw","anonymized")


@pytest.fixture
def raw_bucket(config: ArtifactDownloaderConfig) -> str:
    return config.raw_bucket


@pytest.fixture
def temporary_bucket() -> str:
    return "dev-rcd-temporary-metadata-files"


@pytest.fixture
def input_queue_str(config: ArtifactDownloaderConfig) -> str:
    return config.input_queue


@pytest.fixture
def endpoint(request, config: ArtifactDownloaderConfig) -> str:
    return str(config.artifact_base_url) + request.param


@pytest.fixture
def mock_requests(config: ArtifactDownloaderConfig) -> requests_mock.Mocker:
    with requests_mock.Mocker() as mocker:
        yield mocker


@pytest.fixture
def moto_s3_client(raw_bucket: str, anon_bucket: str, temporary_bucket: str) -> S3Client:
    with mock_s3():
        moto_client = boto3.client("s3", region_name=AWS_REGION)
        moto_client.create_bucket(Bucket=raw_bucket)
        moto_client.create_bucket(Bucket=anon_bucket)
        moto_client.create_bucket(Bucket=temporary_bucket)
        yield moto_client


@pytest.fixture
def moto_sqs_client(input_queue_str: str) -> SQSClient:
    with mock_sqs():
        moto_client = boto3.client("sqs", region_name=AWS_REGION)
        moto_client.create_queue(QueueName=input_queue_str)
        yield moto_client


@pytest.fixture
def input_queue_controller(moto_sqs_client: SQSClient, input_queue_str: str) -> SQSController:
    return SQSController(input_queue_str, moto_sqs_client)


@pytest.fixture
def s3_state_loader(moto_s3_client: S3Client) -> S3StateLoader:
    s3_cloud_state_path = get_abs_path(__file__, "data/s3_cloud_state")
    s3_file_content = get_abs_path(__file__, "data/s3_cloud_state/file_content")
    return S3StateLoader(s3_cloud_state_path, s3_file_content, moto_s3_client)


@pytest.fixture(scope="function")
@patch("artifact_downloader.bootstrap.GracefulExit")
def run_bootstrap(graceful_exit: Mock, config_path: str, input_queue_controller: SQSController):

    di.clear_cache()
    os.environ["AWS_REGION"] = AWS_REGION
    os.environ["CONFIG_PATH"] = config_path

    type(graceful_exit).continue_running = PropertyMock(side_effect=[True, False])
    di[GracefulExit] = graceful_exit
    di[SQSController] = input_queue_controller
    bootstrap_di()


@pytest.fixture
def main_function(run_bootstrap) -> main:
    return main
