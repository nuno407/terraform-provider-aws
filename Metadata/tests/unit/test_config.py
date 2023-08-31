from unittest.mock import patch

import pytest

from metadata.consumer.config import MetadataConfig, DatasetConfig


@pytest.mark.unit
def test_load_valid_config():
    # GIVEN
    raw_config = {
            "create_dataset_for": [{"name":"datanauts", "tenants":["tenant2"]},{"name":"datanauts2", "tenants":["tenant2"]}],
            "default_dataset": "the-default-dataset",
            "tag": "LUX"
    }

    with patch("yaml.safe_load", return_value=raw_config):
        # WHEN
        config = MetadataConfig.load_yaml_config("config/config.yml")
        print(config)

        # THEN
        assert config.create_dataset_for == [DatasetConfig(name="datanauts", tenants=["tenant2"]),DatasetConfig(name="datanauts2", tenants=["tenant2"])]
        assert config.default_dataset == "the-default-dataset"
        assert config.tag == "LUX"

@pytest.mark.unit
def test_load_invalid_config():
    # GIVEN
    raw_config = {
            "wrong-attribute1": ["tenant1", "tenant2"],
            "wrong-attribute2": "the-default-dataset",
            "wrong-attribute3": "LUX"
    }

    with patch("yaml.safe_load", return_value=raw_config):
        # WHEN
        config = MetadataConfig.load_yaml_config("config/config.yml")

        # THEN
        assert config.create_dataset_for == []
        assert config.default_dataset == "Debug_Lync"
        assert config.tag == "RC"
