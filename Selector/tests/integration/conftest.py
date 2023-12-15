import os
from typing import Callable, Generator
from unittest.mock import Mock, PropertyMock

import boto3
import pytest
from kink import di
from moto import mock_s3, mock_sqs  # type: ignore
from mypy_boto3_s3 import S3Client
from mypy_boto3_sqs import SQSClient
from mongoengine import connect
import mongomock

from base.aws.container_services import ContainerServices
from base.aws.s3 import S3Controller
from base.aws.sqs import SQSController
from base.graceful_exit import GracefulExit
from base.testing.utils import get_abs_path
from selector.config import SelectorConfig
from selector.footage_api_token_manager import FootageApiTokenManager
from selector.footage_api_wrapper import FootageApiWrapper
from selector.main import main
from selector.rules.ruleset import ruleset

REGION_NAME = "us-east-1"

CURRENT_LOCATION = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))
TEST_DATA_LOCATION = os.path.join(CURRENT_LOCATION, "data")


@pytest.fixture()
def dev_input_bucket() -> str:
    return "dev-rcd-temporary-metadata-files"


@pytest.fixture()
def moto_s3_client(dev_input_bucket: str) -> Generator[S3Client, None, None]:
    """

    Returns:
        Mock: mocked S3 client
    """
    with mock_s3():
        moto_client = boto3.client("s3", region_name=REGION_NAME)
        moto_client.create_bucket(Bucket=dev_input_bucket)

        yield moto_client


@pytest.fixture()
def dev_input_queue_name() -> str:
    return "dev-terraform-queue-selector"


@pytest.fixture()
def dev_metadata_queue_name() -> str:
    return "dev-terraform-queue-metadata"


@pytest.fixture()
def moto_sqs_client(dev_input_queue_name, dev_metadata_queue_name) -> Generator[SQSClient, None, None]:
    """

    Returns:
        Mock: mocked S3 client
    """
    with mock_sqs():
        # moto only accepts us-east-1
        # https://github.com/spulec/moto/issues/3292#issuecomment-718116897
        moto_client = boto3.client("sqs", region_name=REGION_NAME)
        moto_client.create_queue(QueueName=dev_input_queue_name)
        moto_client.create_queue(QueueName=dev_metadata_queue_name)
        yield moto_client


@pytest.fixture()
def dev_input_queue_url(moto_sqs_client, dev_input_queue_name) -> str:
    return moto_sqs_client.get_queue_url(QueueName=dev_input_queue_name)["QueueUrl"]


@pytest.fixture()
def dev_metadata_controller(moto_sqs_client, dev_metadata_queue_name) -> str:
    return SQSController(dev_metadata_queue_name, moto_sqs_client)


@pytest.fixture
def container_services() -> ContainerServices:
    container = "SDRetriever"
    version = "Mock"

    os.environ["AWS_CONFIG"] = get_abs_path(__file__, "data/config.yaml")
    cont_services = ContainerServices(container, version)
    cont_services.load_config_vars()
    return cont_services


@pytest.fixture()
def graceful_exit() -> GracefulExit:
    exit_mock = Mock()
    return exit_mock


@pytest.fixture()
def client_id() -> str:
    return "mock_client_id"


@pytest.fixture()
def client_secret() -> str:
    return "mock_client_secret"


@pytest.fixture()
def footage_api_url() -> str:
    return "footage_api_url"


@pytest.fixture()
def footage_manager() -> FootageApiTokenManager:
    return Mock()


@pytest.fixture()
def footage_api(footage_api_url, footage_manager) -> FootageApiWrapper:
    return FootageApiWrapper(footage_api_url, footage_manager)


@pytest.fixture()
def run_bootstrap(
        moto_s3_client: S3Client,
        moto_sqs_client: SQSClient,
        container_services: ContainerServices,
        graceful_exit: GracefulExit,
        client_id,
        client_secret,
        footage_api_url: str,
        footage_manager: FootageApiTokenManager,
        footage_api: FootageApiWrapper,
        dev_metadata_controller: SQSController):

    di.clear_cache()
    di[FootageApiWrapper] = footage_api

    # Create the necessary clients for AWS services access
    di[SQSClient] = moto_sqs_client
    di[S3Client] = moto_s3_client

    # Start the S3Controller used for retrieving and deleting from S3
    di[S3Controller] = S3Controller()
    di[FootageApiTokenManager] = footage_manager
    di[ContainerServices] = container_services
    di[GracefulExit] = graceful_exit

    di[SelectorConfig] = SelectorConfig.model_validate({
        "max_GB_per_device_per_month": 2,
        "total_GB_per_month": 100,
        "upload_window_seconds_start": 300,
        "upload_window_seconds_end": 300
    })

    connect(host="mongomock://localhost", db="mongoenginetest", alias="SelectorDB", tz_aware=True)

    di["client_id"] = client_id
    di["client_secret"] = client_secret
    di["token_endpoint"] = container_services.api_endpoints["selector_token_endpoint"]
    di["footage_api_url"] = footage_api_url
    di["default_sqs_queue_name"] = container_services.sqs_queues_list["Selector"]
    di["sqs_metadata_controller"] = dev_metadata_controller
    di["ruleset"] = ruleset()


@pytest.fixture
def main_function(run_bootstrap) -> Callable:
    return main
