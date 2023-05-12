from typing import Generator, Any
from moto import mock_s3, mock_sqs
import pytest
import os
import boto3
import json
from base.aws.container_services import ContainerServices
from mypy_boto3_s3 import S3Client
from mypy_boto3_sqs import SQSClient

from mdfparser.config import MdfParserConfig
from mdfparser.consumer import Consumer
from mdfparser.imu.downloader import IMUDownloader
from mdfparser.imu.handler import IMUHandler
from mdfparser.imu.transformer import IMUTransformer
from mdfparser.imu.uploader import IMUUploader
from mdfparser.metadata.downloader import MetadataDownloader
from mdfparser.metadata.handler import MetadataHandler
from mdfparser.metadata.synchronizer import Synchronizer
from mdfparser.metadata.uploader import MetadataUploader

from base.aws.container_services import ContainerServices
from base.chc_counter import ChcCounter
from base.gnss_coverage import GnssCoverage
from base.max_audio_loudness import MaxAudioLoudness
from base.max_person_count import MaxPersonCount
from base.mean_audio_bias import MeanAudioBias
from base.median_person_count import MedianPersonCount
from base.ride_detection_people_count_before import RideDetectionPeopleCountBefore
from base.ride_detection_people_count_after import RideDetectionPeopleCountAfter
from base.sum_door_closed import SumDoorClosed
from base.variance_person_count import VariancePersonCount

from mdfparser.config import MdfParserConfig
from mypy_boto3_s3 import S3Client
from mypy_boto3_sqs import SQSClient

CURRENT_LOCATION = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))
S3_DATA = os.path.join(CURRENT_LOCATION, "data", "s3_data")
SQS_MESSAGES = os.path.join(CURRENT_LOCATION, "data", "sqs_messages")
AWS_CONFIG_PATH = os.path.join(CURRENT_LOCATION, "data", "config", "aws_config.yaml")
MDFP_CONFIG_PATH = os.path.join(CURRENT_LOCATION, "data", "config", "mdfp_config.yaml")
REGION_NAME = "us-east-1"


def get_raw_file(path: str) -> bytearray:
    """
    Read raw file.

    Args:
        path (str): path to file.

    Returns:
        bytearray: File content
    """
    with open(path, "rb") as f:
        return f.read()


@pytest.fixture(scope="function")
def dev_input_bucket() -> str:
    return "dev-rcd-raw-video-files"


@pytest.fixture(scope="function")
def dev_output_bucket() -> str:
    return "dev-rcd-temporary-metadata-files"


@pytest.fixture(scope="function")
def moto_s3_client(dev_input_bucket: str, dev_output_bucket: str) -> Generator[S3Client, None, None]:
    """

    Returns:
        Mock: mocked S3 client
    """
    with mock_s3():
        moto_client = boto3.client("s3", region_name=REGION_NAME)
        moto_client.create_bucket(Bucket=dev_input_bucket)
        moto_client.create_bucket(Bucket=dev_output_bucket)

        yield moto_client


def sqs_message(sqs_message_name) -> dict[Any, Any]:
    """
    Load json message test artifact

    Args:
        sqs_message_name (str): File name without extension

    Returns:
        dict[Any, Any]: Parsed file
    """
    file_name = sqs_message_name + ".json"
    file_path = os.path.join(SQS_MESSAGES, file_name)
    data = get_raw_file(file_path).decode("utf-8")
    return json.loads(data)


@pytest.fixture(scope="function")
def moto_sqs_client() -> Generator[SQSClient, None, None]:
    """

    Returns:
        Mock: mocked S3 client
    """
    with mock_sqs():
        # moto only accepts us-east-1 https://github.com/spulec/moto/issues/3292#issuecomment-718116897
        moto_client = boto3.client("sqs", region_name=REGION_NAME)
        yield moto_client


@pytest.fixture()
def dev_output_queue_url(moto_sqs_client) -> str:
    moto_sqs_client.create_queue(QueueName="dev-terraform-queue-metadata")
    return moto_sqs_client.get_queue_url(QueueName="dev-terraform-queue-metadata")["QueueUrl"]


@pytest.fixture()
def dev_input_queue_url(moto_sqs_client) -> str:
    moto_sqs_client.create_queue(QueueName="dev-terraform-queue-mdf-parser")
    return moto_sqs_client.get_queue_url(QueueName="dev-terraform-queue-mdf-parser")["QueueUrl"]


@pytest.fixture()
def mdf_parser_config() -> MdfParserConfig:
    """ Return a MdfParserConfig for all tests in this class """
    return MdfParserConfig(
        input_queue="dev-terraform-queue-mdf-parser",
        metadata_output_queue="dev-terraform-queue-metadata",
        temporary_bucket="dev-rcd-temporary-metadata-files"
    )


@pytest.fixture()
def consumer_mocks(moto_sqs_client: SQSClient, moto_s3_client: S3Client,
                   mdf_parser_config: MdfParserConfig) -> tuple[Consumer, SQSClient, S3Client]:
    os.environ["REGION_NAME"] = REGION_NAME
    os.environ["AWS_CONFIG"] = AWS_CONFIG_PATH

    container_services = ContainerServices(container="MDFParser", version="test_version")

    imu_downloader = IMUDownloader(container_services, moto_s3_client)
    imu_uploader = IMUUploader(container_services, moto_s3_client)
    imu_transformer = IMUTransformer()
    imu_handler = IMUHandler(imu_downloader, imu_uploader, imu_transformer, mdf_parser_config)

    processor_list = [
        ChcCounter(),
        GnssCoverage(),
        MaxAudioLoudness(),
        MaxPersonCount(),
        MeanAudioBias(),
        MedianPersonCount(),
        VariancePersonCount(),
        RideDetectionPeopleCountBefore(),
        RideDetectionPeopleCountAfter(),
        SumDoorClosed()
    ]

    metadata_downloader = MetadataDownloader(container_services, moto_s3_client)
    metadata_uploader = MetadataUploader(container_services, moto_s3_client)
    metadata_sync = Synchronizer()
    metadata_handler = MetadataHandler(metadata_downloader, metadata_uploader, metadata_sync, processor_list)

    consumer = Consumer(
        container_services,
        [metadata_handler, imu_handler],
        mdf_parser_config,
        moto_sqs_client)

    return consumer, moto_sqs_client, moto_s3_client
