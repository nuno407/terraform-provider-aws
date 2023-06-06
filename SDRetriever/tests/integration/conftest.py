# type: ignore
import pytest
import boto3
import os
from moto import mock_s3, mock_sqs, mock_sts
from kink import di
from typing import Generator, Callable

from mypy_boto3_s3 import S3Client
from mypy_boto3_sts import STSClient
from mypy_boto3_sqs import SQSClient
from unittest.mock import Mock, PropertyMock
from sdretriever.metadata_merger import MetadataMerger
from sdretriever.config import SDRetrieverConfig
from sdretriever.s3_finder_rcc import S3FinderRCC
from sdretriever.main import main
from base.testing.utils import get_abs_path
from base.aws.container_services import ContainerServices
from base.aws.s3 import S3ClientFactory, S3Controller, S3ControllerFactory
from base.aws.shared_functions import StsHelper
from base.graceful_exit import GracefulExit
from base.testing.utils import get_abs_path
from typing import Generator, Callable


@pytest.fixture
def region_name() -> str:
    return "us-east-1"


@pytest.fixture
def rcc_bucket() -> str:
    return "rcc-dev-device-data"


@pytest.fixture
def devcloud_temporary_bucket() -> str:
    return "tmp_bucket"


@pytest.fixture
def devcloud_bucket() -> str:
    return "dev-rcd-raw-video-files"


@pytest.fixture
def download_queue() -> str:
    return "dev-terraform-queue-download"


@pytest.fixture
def selector_queue() -> str:
    return "dev-terraform-queue-selector"


@pytest.fixture
def all_queues(download_queue: str, selector_queue: str) -> list[str]:
    queues = [
        "dev-terraform-queue-s3-sdm",
        "dev-terraform-queue-anonymize",
        "dev-terraform-queue-api-anonymize",
        "dev-terraform-queue-chc",
        "dev-terraform-queue-api-chc",
        download_queue,
        selector_queue,
        "dev-terraform-queue-metadata",
        "dev-terraform-queue-output",
        "dev-terraform-queue-mdf-parser"
    ]
    return queues


@pytest.fixture
def config(devcloud_temporary_bucket: str) -> SDRetrieverConfig:
    """Config for testing."""
    return SDRetrieverConfig(
        tenant_blacklist=[],
        recorder_blacklist=[],
        frame_buffer=0,
        training_whitelist=[],
        request_training_upload=True,
        discard_video_already_ingested=True,
        ingest_from_kinesis=True,
        input_queue="queue",
        temporary_bucket=devcloud_temporary_bucket
    )


@pytest.fixture
def moto_s3_client(region_name: str, rcc_bucket: str, devcloud_bucket: str,
                   config: SDRetrieverConfig) -> Generator[S3Client, None, None]:
    """

    Returns:
        Mock: mocked S3 client
    """
    with mock_s3():
        moto_client = boto3.client("s3", region_name=region_name)
        moto_client.create_bucket(Bucket=rcc_bucket)
        moto_client.create_bucket(Bucket=devcloud_bucket)
        moto_client.create_bucket(Bucket=config.temporary_bucket)
        yield moto_client


@pytest.fixture
def moto_sqs_client(region_name: str, all_queues: list[str]) -> Generator[SQSClient, None, None]:
    """

    Returns:
        Mock: mocked S3 client
    """
    with mock_sqs():
        moto_client = boto3.client("sqs", region_name=region_name)
        for queue in all_queues:
            moto_client.create_queue(QueueName=queue)
        yield moto_client


@pytest.fixture
def moto_sts_client(region_name: str) -> Generator[STSClient, None, None]:
    """

    Returns:
        Mock: mocked S3 client
    """
    with mock_sts():
        moto_client = boto3.client("sts", region_name=region_name)
        yield moto_client


@pytest.fixture
def container_services() -> ContainerServices:
    container = "SDRetriever"
    version = "Mock"

    os.environ["AWS_CONFIG"] = get_abs_path(__file__, "data/config.yaml")
    cont_services = ContainerServices(container, version)
    cont_services.load_config_vars()
    return cont_services


@pytest.fixture
def sts_helper():
    # TODO: Handle credential exchange
    return Mock()


@pytest.fixture
def one_time_gracefull_exit() -> GracefulExit:
    exit = Mock()
    type(exit).continue_running = PropertyMock(side_effect=[True, False])
    return exit


@pytest.fixture
def rcc_s3_client_factory(moto_s3_client: S3Client) -> S3ControllerFactory:
    def mocked_client_factory():
        return moto_s3_client
    return mocked_client_factory


@pytest.fixture
def rcc_client_factory(moto_s3_client: S3Client, rcc_bucket: str) -> S3ClientFactory:
    moto_s3_client.create_bucket(Bucket=rcc_bucket)

    def mocked_client_factory():
        return moto_s3_client

    return mocked_client_factory


@pytest.fixture
def rcc_s3_controller_factory(moto_s3_client: S3Client) -> S3ControllerFactory:
    def mocked_client_factory():
        return S3Controller(moto_s3_client)
    return mocked_client_factory


@pytest.fixture
def run_bootstrap(
        config: SDRetrieverConfig,
        container_services: ContainerServices,
        rcc_s3_controller_factory: S3ControllerFactory,
        rcc_s3_client_factory: S3ClientFactory,
        moto_s3_client: SQSClient,
        moto_sqs_client: SQSClient,
        moto_sts_client: STSClient,
        rcc_bucket: str,
        one_time_gracefull_exit: GracefulExit,
        download_queue: str):
    di[SDRetrieverConfig] = config  # Config loading will not be tested

    # string constants
    di["default_sqs_queue_name"] = config.input_queue

    # boto3 clients
    di[SQSClient] = moto_sqs_client
    di[S3Client] = moto_s3_client
    di[STSClient] = moto_sts_client

    # base aws services
    di[ContainerServices] = container_services
    di[ContainerServices].configure_logging("SDRetriever")
    di[StsHelper] = Mock()  # Needed for kinesis stream

    # di[StsHelper] = StsHelper(
    #    di[STSClient],
    #    role=di[ContainerServices].rcc_info.get("role"),
    #    role_session="DevCloud-SDRetriever")

    # di["rcc_bucket"] = di[ContainerServices].rcc_info["s3_bucket"]
    di["rcc_bucket"] = rcc_bucket
    di["default_sqs_queue_name"] = download_queue

    # rcc boto3 clients
    di[S3ClientFactory] = rcc_s3_client_factory
    di[S3ControllerFactory] = rcc_s3_controller_factory

    # graceful exit
    di[GracefulExit] = one_time_gracefull_exit
    di[S3FinderRCC] = S3FinderRCC()
    di[MetadataMerger] = MetadataMerger()


@pytest.fixture
def main_function(run_bootstrap) -> Callable:
    return main
