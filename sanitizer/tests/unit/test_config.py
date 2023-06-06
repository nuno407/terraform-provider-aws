""" Unit tests for the config module. """
import pytest

from base.model.artifacts import IMUArtifact
from sanitizer.config import SanitizerConfig


@pytest.mark.unit
def test_load_yaml_config():
    """ Test load_yaml_config method. """
    raw_config = """
    input_queue: test-queue
    topic_arn: test-topic
    db_name: test-db
    message_collection: test-incoming-messages
    tenant_blacklist:
    - onetenant
    recorder_blacklist:
    - FrontRecorder
    type_blacklist:
    - IMUArtifact
    """
    # write to a temporary file
    with open("/tmp/config1", "w", encoding="utf-8") as file_handler:
        file_handler.write(raw_config)
    config = SanitizerConfig.load_yaml_config("/tmp/config1")
    assert config.db_name == "test-db"
    assert config.input_queue == "test-queue"
    assert config.topic_arn == "test-topic"
    assert config.recorder_blacklist == ["FrontRecorder"]
    assert config.tenant_blacklist == ["onetenant"]
    assert config.message_collection == "test-incoming-messages"
    assert config.type_blacklist == {IMUArtifact}


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
