# pylint: disable=E1120
"""bootstrap dependency injection autowiring."""
import os

from metadata.consumer.config import MetadataConfig, DatasetMappingConfig
from kink import di


def bootstrap_di() -> None:
    """Initializes dependency injection autowiring container."""

    di["config_path"] = os.getenv("TENANT_MAPPING_CONFIG_PATH", "/app/config/config.yml")

    config = MetadataConfig.load_config_from_yaml_file(di["config_path"])
    di[MetadataConfig] = config
    di[DatasetMappingConfig] = config.dataset_mapping
