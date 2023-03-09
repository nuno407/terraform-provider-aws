from unittest.mock import patch

import pytest

from metadata.consumer.config import MetadataConfig, DatasetMappingConfig


@pytest.mark.unit
def test_load_valid_config():
    # GIVEN
    raw_config = {
        "dataset_mapping": {
            "create_dataset_for": {"tenant1", "tenant2"},
            "default_dataset": "the-default-dataset",
            "tag": "LUX",
            "ignored": "value"
        }
    }

    with patch("yaml.safe_load", return_value=raw_config):
        # WHEN
        config = MetadataConfig.load_config_from_yaml_file("config/config.yml")

        # THEN
        assert config.dataset_mapping.create_dataset_for == {"tenant1", "tenant2"}
        assert config.dataset_mapping.default_dataset == "the-default-dataset"
        assert config.dataset_mapping.tag == "LUX"
        assert not hasattr(config.dataset_mapping, "ignored")


@pytest.mark.unit
def test_load_invalid_dataset_config():
    # GIVEN
    raw_config = {
        "dataset_mapping": {
            "wrong-attribute1": ["tenant1", "tenant2"],
            "wrong-attribute2": "the-default-dataset",
            "wrong-attribute3": "LUX"
        }
    }

    with patch("yaml.safe_load", return_value=raw_config):
        # WHEN
        config = MetadataConfig.load_config_from_yaml_file("config/config.yml")

        # THEN
        assert config.dataset_mapping.create_dataset_for == set()
        assert config.dataset_mapping.default_dataset == ""
        assert config.dataset_mapping.tag == ""


@pytest.mark.unit
def test_load_invalid_config():
    # GIVEN
    raw_config = {
        "not-relevant": {
            "wrong-attribute1": ["tenant1", "tenant2"],
            "wrong-attribute2": "the-default-dataset",
            "wrong-attribute3": "LUX"
        }
    }

    with patch("yaml.safe_load", return_value=raw_config):
        # WHEN
        config = MetadataConfig.load_config_from_yaml_file("config/config.yml")

        # THEN
        assert config.dataset_mapping.create_dataset_for == set()
        assert config.dataset_mapping.default_dataset == ""
        assert config.dataset_mapping.tag == ""
