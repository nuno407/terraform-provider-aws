import json
import os
from typing import Any, Generator, Callable
import boto3
from kink import di
from mypy_boto3_sqs import SQSClient
from mypy_boto3_s3 import S3Client
from base.aws.s3 import S3Controller
from base.testing.utils import get_abs_path
import pytest
from unittest.mock import Mock, PropertyMock
from selector.selector import Selector
from moto import mock_s3, mock_sqs
from base.aws.container_services import ContainerServices
from base.graceful_exit import GracefulExit
from selector.rules.ruleset import ruleset
from selector.main import main
from selector.footage_api_wrapper import FootageApiWrapper
from selector.footage_api_token_manager import FootageApiTokenManager

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
def moto_sqs_client(dev_input_queue_name) -> Generator[SQSClient, None, None]:
    """

    Returns:
        Mock: mocked S3 client
    """
    with mock_sqs():
        # moto only accepts us-east-1
        # https://github.com/spulec/moto/issues/3292#issuecomment-718116897
        moto_client = boto3.client("sqs", region_name=REGION_NAME)
        moto_client.create_queue(QueueName=dev_input_queue_name)
        yield moto_client


@pytest.fixture()
def dev_input_queue_url(moto_sqs_client, dev_input_queue_name) -> str:
    return moto_sqs_client.get_queue_url(QueueName=dev_input_queue_name)["QueueUrl"]


@pytest.fixture
def container_services() -> ContainerServices:
    container = "SDRetriever"
    version = "Mock"

    os.environ["AWS_CONFIG"] = get_abs_path(__file__, "data/config.yaml")
    cont_services = ContainerServices(container, version)
    cont_services.load_config_vars()
    return cont_services


@pytest.fixture()
def one_time_gracefull_exit() -> GracefulExit:
    exit = Mock()
    type(exit).continue_running = PropertyMock(side_effect=[True, False])
    return exit


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
def run_bootstrap(moto_s3_client: S3Client, moto_sqs_client: SQSClient, container_services: ContainerServices,
                  one_time_gracefull_exit: GracefulExit, client_id, client_secret, footage_api_url: str, footage_manager: FootageApiTokenManager, footage_api: FootageApiWrapper):

    di.clear_cache()
    di[FootageApiWrapper] = footage_api

    # Create the necessary clients for AWS services access
    di[SQSClient] = moto_sqs_client
    di[S3Client] = moto_s3_client

    # Start the S3Controller used for retrieving and deleting from S3
    di[S3Controller] = S3Controller()
    di[FootageApiTokenManager] = footage_manager
    di[ContainerServices] = container_services
    di[GracefulExit] = one_time_gracefull_exit

    di["client_id"] = client_id
    di["client_secret"] = client_secret
    di["token_endpoint"] = container_services.api_endpoints["selector_token_endpoint"]
    di["footage_api_url"] = footage_api_url
    di["default_sqs_queue_name"] = container_services.sqs_queues_list["Selector"]
    di["ruleset"] = ruleset()


@pytest.fixture
def main_function(run_bootstrap) -> Callable:
    return main
