import pytest
from sanitizer.config import SanitizerConfig


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
        type_blacklist={},
        devcloud_raw_bucket="test-raw",
        devcloud_anonymized_bucket="test-anonymized"
    )
