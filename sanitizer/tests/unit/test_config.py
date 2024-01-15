""" Unit tests for the config module. """
import pytest
from base.testing.utils import get_abs_path
from base.model.artifacts import IMUArtifact
from sanitizer.config import SanitizerConfig


@pytest.mark.unit
def test_load_yaml_config():
    """ Test load_yaml_config method. """
    config = SanitizerConfig.load_yaml_config(get_abs_path(__file__, "data/config.yaml"))
    assert config.db_name == "test-db"
    assert config.input_queue == "test-queue"
    assert config.metadata_queue == "mdq"
    assert config.topic_arn == "test-topic"
    assert config.recorder_blacklist == ["FrontRecorder"]
    assert config.tenant_blacklist == ["onetenant"]
    assert config.message_collection == "test-incoming-messages"
    assert config.type_blacklist == {IMUArtifact}
    assert config.device_info_collection == "device_info_collection"
    assert config.version_blacklist == {IMUArtifact: set(["1.8.0"])}
    assert config.devcloud_raw_bucket == "test-raw"
    assert config.devcloud_anonymized_bucket == "test-anonymized"


@pytest.mark.unit
def test_load_yaml_config_exception():
    """ Test load_yaml_config method. """
    raw_config = """
    bad_configuration:
    - FrontRecorder
    """
    # write to a temporary file
    with open("/tmp/config2", "w", encoding="utf-8") as file_handler:
        file_handler.write(raw_config)
    with pytest.raises(Exception):
        SanitizerConfig.load_yaml_config("/tmp/config2")
