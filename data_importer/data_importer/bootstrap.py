"""bootstrap dependency injection autowiring."""
import os

from kink import di

from base.model.config.policy_config import PolicyConfig
from data_importer.models.config import DataImporterConfig


def bootstrap_di() -> None:
    """Initializes dependency injection autowiring container."""

    tenant_config = DataImporterConfig.load_yaml_config(
        os.getenv("TENANT_MAPPING_CONFIG_PATH", "/app/config/config.yml"))
    di[PolicyConfig] = tenant_config.policy_mapping
