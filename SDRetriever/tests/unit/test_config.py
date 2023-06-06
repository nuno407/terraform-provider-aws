from sdretriever.config import SDRetrieverConfig
import pytest


@pytest.mark.unit
def test_config():
    raw_config = """
    tenant_blacklist:
    - onetenant
    recorder_blacklist:
    - FrontRecorder
    frame_buffer: 10
    training_whitelist:
    - onetenant
    request_training_upload: true
    discard_video_already_ingested: true
    ingest_from_kinesis: true
    input_queue: test-queue
    temporary_bucket: tmp
    """
    with open("/tmp/config1", "w", encoding="utf-8") as file_handler:
        file_handler.write(raw_config)
    config = SDRetrieverConfig.load_config_from_yaml_file("/tmp/config1")
    assert config.tenant_blacklist == ["onetenant"]
    assert config.recorder_blacklist == ["FrontRecorder"]
    assert config.frame_buffer == 10
    assert config.training_whitelist == ["onetenant"]
    assert config.request_training_upload
    assert config.discard_video_already_ingested
    assert config.input_queue == "test-queue"
    assert config.temporary_bucket == "tmp"


@pytest.mark.unit
def test_config_with_invalid_field():
    raw_config = """
    bad_configuration:
    - FrontRecorder
    """
    with open("/tmp/config2", "w", encoding="utf-8") as file_handler:
        file_handler.write(raw_config)
    with pytest.raises(Exception):
        SDRetrieverConfig.load_config_from_yaml_file("/tmp/config2")


@pytest.mark.unit
def test_config_with_missing_input_queue_field():
    raw_config = """
    tenant_blacklist:
    - onetenant
    recorder_blacklist:
    - FrontRecorder
    frame_buffer: 10
    training_whitelist:
    - onetenant
    request_training_upload: true
    ingest_from_kinesis: true
    discard_video_already_ingested: true
    """
    with open("/tmp/config3", "w", encoding="utf-8") as file_handler:
        file_handler.write(raw_config)
    with pytest.raises(Exception):
        SDRetrieverConfig.load_config_from_yaml_file("/tmp/config3")
