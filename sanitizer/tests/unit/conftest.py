from unittest.mock import Mock
from sanitizer.device_info_db_client import DeviceInfoDBClient
from sanitizer.config import SanitizerConfig
import pytest


@pytest.fixture
def sanitizer_config():
    return SanitizerConfig(
        db_name="test-db",
        input_queue="test-queue",
        metadata_queue="mdq",
        topic_arn="test-topic",
        recorder_blacklist=["FrontRecorder"],
        tenant_blacklist=["onetenant"],
        message_collection="test-incoming-messages",
        device_info_collection="device_info_collection",
        version_blacklist={},
        type_blacklist={},
        devcloud_raw_bucket="test-raw",
        devcloud_anonymized_bucket="test-anonymized"
    )


@pytest.fixture
def mock_device_db_client() -> DeviceInfoDBClient:
    return Mock()
