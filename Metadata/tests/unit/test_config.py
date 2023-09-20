from unittest.mock import patch

import pytest
from pydantic import ValidationError

from base.model.config.dataset_config import TenantDatasetConfig
from base.testing.utils import get_abs_path
from metadata.consumer.config import MetadataConfig


@pytest.mark.unit
def test_load_valid_config():
    # GIVEN
    raw_config = {
        "dataset_mapping": {
            "create_dataset_for": [{"name": "datanauts", "tenants": ["tenant2"]},
                                   {"name": "datanauts2", "tenants": ["tenant2"]}],
            "default_dataset": "the-default-dataset",
            "tag": "LUX"
        },
        "policy_mapping": {
            "default_policy_document": "unused",
            "policy_document_per_tenant": {"datanauts": "top-secret"},
        }
    }

    with patch("yaml.safe_load", return_value=raw_config):
        # WHEN
        config = MetadataConfig.load_yaml_config(get_abs_path(__file__, "test_data/mongo_config.yml"))

        # THEN
        dataset_config = config.dataset_mapping
        assert dataset_config.create_dataset_for == [
            TenantDatasetConfig(name="datanauts", tenants=["tenant2"]),
            TenantDatasetConfig(name="datanauts2", tenants=["tenant2"])
        ]
        assert dataset_config.default_dataset == "the-default-dataset"
        assert dataset_config.tag == "LUX"

        policy_config = config.policy_mapping
        assert policy_config.default_policy_document == "unused"
        assert policy_config.policy_document_per_tenant == {"datanauts": "top-secret"}


@pytest.mark.unit
def test_load_additional_properties_config():
    # GIVEN
    raw_config = {
        "dataset_mapping": {
            "create_dataset_for": [{"name": "datanauts", "tenants": ["tenant2"]},
                                   {"name": "datanauts2", "tenants": ["tenant2"]}],
            "default_dataset": "the-default-dataset",
            "tag": "LUX"
        },
        "policy_mapping": {
            "default_policy_document": "unused",
        },
        "uninteresting": {
            "other": "stuff"
        }
    }

    with patch("yaml.safe_load", return_value=raw_config):
        # WHEN
        config = MetadataConfig.load_yaml_config(get_abs_path(__file__, "test_data/mongo_config.yml"))

        # THEN
        dataset_config = config.dataset_mapping
        assert dataset_config.create_dataset_for == [
            TenantDatasetConfig(name="datanauts", tenants=["tenant2"]),
            TenantDatasetConfig(name="datanauts2", tenants=["tenant2"])
        ]
        assert dataset_config.default_dataset == "the-default-dataset"
        assert dataset_config.tag == "LUX"

        policy_config = config.policy_mapping
        assert policy_config.default_policy_document == "unused"
        assert policy_config.policy_document_per_tenant == {}


@pytest.mark.unit
def test_load_invalid_config():
    # GIVEN
    raw_config = {
        "dataset_mapping": {
            "wrong-attribute1": ["tenant1", "tenant2"],
            "wrong-attribute2": "the-default-dataset",
            "wrong-attribute3": "LUX",
        }
    }

    with patch("yaml.safe_load", return_value=raw_config):
        # WHEN
        with pytest.raises(ValidationError):
            MetadataConfig.load_yaml_config(get_abs_path(__file__, "test_data/mongo_config.yml"))
