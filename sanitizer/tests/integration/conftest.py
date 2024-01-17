import os
import logging
from typing import Generator, Callable
import boto3
from kink import di
import pytest
from moto import mock_sns, mock_sqs  # type: ignore
from unittest.mock import Mock, PropertyMock
from base.graceful_exit import GracefulExit

from sanitizer.artifact.parsers.snapshot_parser import SnapshotParser
from sanitizer.artifact.parsers.s3_video_parser import S3VideoParser
from sanitizer.artifact.parsers.operator_feedback_parser import OperatorFeedbackParser
from sanitizer.artifact.parsers.event_parser import EventParser
from sanitizer.device_info_db_client import DeviceInfoDBClient

from mypy_boto3_sns import SNSClient
from mypy_boto3_sqs import SQSClient
from mongomock import MongoClient as MongoClientMock
from pymongo import MongoClient

from base.aws.sqs import SQSController
from base.testing.utils import get_abs_path
from base.graceful_exit import GracefulExit
from sanitizer.config import SanitizerConfig
from sanitizer.main import main

REGION_NAME = "us-east-1"


@pytest.fixture()
def input_queue() -> str:
    return "dev-terraform-queue-sanitizer"


@pytest.fixture()
def metadata_queue() -> str:
    return "dev-terraform-queue-metadata"


@pytest.fixture()
def test_output_queue() -> str:
    return "dev-terraform-output-queue"


@pytest.fixture()
def output_topic_name() -> str:
    return "dev-sanitizer-output-topic"


@pytest.fixture()
def message_collection() -> str:
    return "dev-incoming-messages"


@pytest.fixture()
def db_name() -> str:
    return "DataIngestion"


@pytest.fixture()
def moto_sqs_client(input_queue: str, metadata_queue: str, test_output_queue: str) -> Generator[SQSClient, None, None]:
    """

    Returns:
        Mock: mocked S3 client
    """
    with mock_sqs():
        moto_client = boto3.client("sqs", region_name=REGION_NAME)
        moto_client.create_queue(QueueName=input_queue)
        moto_client.create_queue(QueueName=metadata_queue)
        moto_client.create_queue(QueueName=test_output_queue)
        yield moto_client


@pytest.fixture()
def moto_sns_client(moto_sqs_client: SQSClient, output_topic_name: str, test_output_queue: str) -> SNSClient:  # pylint: disable=redefined-outer-name
    """Receives moto3 mocked sqs client and uses it to create
       SQS queue and subscribe this queue to a created SNS topic"""
    with mock_sns():

        # Output queue
        queue_reponse = moto_sqs_client.get_queue_url(QueueName=test_output_queue)
        queue_url = queue_reponse["QueueUrl"]
        attributes = moto_sqs_client.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["All"])
        sqs_queue_arn = attributes["Attributes"]["QueueArn"]
        msns_client = boto3.client("sns", region_name="us-east-1")
        topic_response = msns_client.create_topic(Name=output_topic_name)
        sns_topic_arn = topic_response["TopicArn"]
        msns_client.subscribe(
            TopicArn=sns_topic_arn,
            Protocol="sqs",
            Endpoint=sqs_queue_arn
        )

        yield msns_client  # NOSONAR


@pytest.fixture()
def input_sqs_controller(input_queue: str, moto_sqs_client: SQSClient) -> SQSController:
    return SQSController(input_queue, moto_sqs_client)


@pytest.fixture()
def metadata_sqs_controller(metadata_queue: str, moto_sqs_client: SQSClient) -> SQSController:
    return SQSController(metadata_queue, moto_sqs_client)


@pytest.fixture()
def output_sqs_controller(test_output_queue: str, moto_sqs_client: SQSClient) -> SQSController:
    return SQSController(test_output_queue, moto_sqs_client)


@pytest.fixture()
def mongo_client() -> MongoClientMock:
    return MongoClientMock()


@pytest.fixture()
def output_topic_arn(moto_sns_client: SNSClient, output_topic_name: str) -> str:
    topic_response = moto_sns_client.create_topic(Name=output_topic_name)
    return topic_response["TopicArn"]


@pytest.fixture()
def gracefull_exit() -> GracefulExit:
    exit = Mock()
    type(exit).continue_running = PropertyMock(side_effect=[True, False])
    return exit


@pytest.fixture()
def config(request, input_queue: str,
           metadata_queue: str,
           output_topic_arn: str,
           message_collection: str,
           db_name: str) -> SanitizerConfig:

    if not hasattr(request, "param") or request.param is None:
        return SanitizerConfig(
            input_queue=input_queue,
            metadata_queue=metadata_queue,
            topic_arn=output_topic_arn,
            message_collection=message_collection,
            db_name=db_name,
            tenant_blacklist=[],
            recorder_blacklist=[],
            type_blacklist=[],
            version_blacklist={},
            device_info_collection="device_info_collection",
            devcloud_raw_bucket="test-raw",
            devcloud_anonymized_bucket="test-anonymized"
        )
    else:
        remote_config = SanitizerConfig.load_yaml_config(get_abs_path(__file__, os.path.join("data", request.param)))
        remote_config.input_queue = input_queue
        remote_config.metadata_queue = metadata_queue
        remote_config.topic_arn = output_topic_arn
        remote_config.message_collection = message_collection
        remote_config.db_name = db_name
        return remote_config


@pytest.fixture()
def device_info_client(mongo_client: MongoClientMock, config: SanitizerConfig) -> DeviceInfoDBClient:
    return DeviceInfoDBClient(device_info_collection=mongo_client[config.db_name][config.device_info_collection])


def configure_logging(component_name: str) -> logging.Logger:
    logging.basicConfig(
        format="%(asctime)s %(name)s\t%(levelname)s\t%(message)s", level=logging.DEBUG)
    logging.getLogger("base").setLevel(logging.DEBUG)
    logger = logging.getLogger(component_name)
    logger.setLevel(logging.DEBUG)
    return logger


@pytest.fixture()
def run_bootstrap(
        moto_sns_client: SNSClient,
        moto_sqs_client: SQSClient,
        gracefull_exit: GracefulExit,
        config: SanitizerConfig,
        mongo_client: MongoClientMock,
        device_info_client: DeviceInfoDBClient,
        metadata_sqs_controller: SQSController,
        input_sqs_controller: SQSController):

    di.clear_cache()
    configure_logging("sanitizer")

    # useful for local testing
    di["start_delay"] = 0
    di[SanitizerConfig] = config
    di["container_name"] = "Sanitizer"
    di["default_sns_topic_arn"] = config.topic_arn
    di["default_sqs_queue_name"] = config.input_queue
    di[DeviceInfoDBClient] = device_info_client

    di[logging.Logger] = logging.getLogger("sanitizer")
    di[GracefulExit] = gracefull_exit

    di[MongoClient] = mongo_client

    di[SQSClient] = moto_sqs_client
    di[SNSClient] = moto_sns_client

    di["metadata_sqs_controller"] = metadata_sqs_controller
    di["aws_sqs_rcc_controller"] = input_sqs_controller

    di[SnapshotParser] = SnapshotParser()
    di[S3VideoParser] = S3VideoParser()
    di[OperatorFeedbackParser] = OperatorFeedbackParser()
    di[EventParser] = EventParser()


@pytest.fixture
def main_function(run_bootstrap) -> Callable:
    return main
