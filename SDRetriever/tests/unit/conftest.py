# type: ignore
import re
from unittest.mock import Mock, PropertyMock
from pytest import fixture
from mypy_boto3_s3 import S3Client
from sdretriever.config import SDRetrieverConfig
from base.aws.container_services import ContainerServices
from base.aws.s3 import S3ClientFactory, S3Controller
from sdretriever.s3_finder_rcc import S3FinderRCC
from sdretriever.metadata_merger import MetadataMerger
from sdretriever.s3_crawler_rcc import S3CrawlerRCC
from sdretriever.metadata_merger import MetadataMerger


@fixture()
def config() -> SDRetrieverConfig:
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
        temporary_bucket="tmp_bucket"
    )


@fixture()
def metadata_merger() -> MetadataMerger:
    return MetadataMerger()


@fixture()
def raw_s3() -> str:
    return "raw-s3"


@fixture()
def rcc_bucket() -> str:
    return "test-rcc-bucket"


@fixture()
def container_services(raw_s3, rcc_bucket) -> ContainerServices:
    container_services = Mock()
    type(container_services).raw_s3 = PropertyMock(return_value=raw_s3)
    type(container_services).rcc_info = {
        "s3_bucket": rcc_bucket,
    }
    return container_services


@fixture()
def rcc_client_factory() -> S3ClientFactory:
    return Mock()


@fixture()
def s3_controller() -> S3Controller:
    """S3Controller for testing."""
    s3_controller = Mock()
    s3_controller.upload_file = Mock()
    return s3_controller


@fixture()
def s3_finder() -> S3FinderRCC:
    return Mock()


@fixture()
def s3_client() -> S3Client:
    return Mock()


@fixture()
def s3_crawler() -> S3CrawlerRCC:
    return Mock()


@fixture()
def metadata_merger() -> MetadataMerger:
    return MetadataMerger()


@fixture()
def video_pattern() -> re.Pattern:
    return re.compile(r"([^\W_]+)_([^\W_]+)-([a-z0-9\-]+)_(\d+)\.mp4$")


@fixture()
def image_pattern() -> re.Pattern:
    return re.compile(r"([^\W_]+)_([^\W_]+)-([a-z0-9\-]+)_(\d+)\.jpeg$")


@fixture
def s3_client_factory():
    s3_client = Mock()

    def s3_controller_factory():
        return s3_client
    return s3_controller_factory


@fixture
def s3_controller_factory():
    s3_client = Mock()

    def s3_controller_factory():
        return s3_client
    return s3_controller_factory


@fixture
def s3_controller(s3_controller_factory: Mock) -> Mock:
    return s3_controller_factory()
