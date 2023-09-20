"""Metadata config loaded from config file"""

import yaml
from pydantic import BaseModel

from base.model.config.dataset_config import DatasetConfig
from base.model.config.policy_config import PolicyConfig


class MetadataConfig(BaseModel):
    """
    Config holding information about Metadata component
    """
    dataset_mapping: DatasetConfig
    policy_mapping: PolicyConfig

    @staticmethod
    def load_yaml_config(config_path: str) -> "MetadataConfig":
        """load_yaml_config.

        Loads YAML configuration file form path

        Args:
            config_path (str): path to yaml file

        Returns:
            SanitizerConfig: configuration
        """
        with open(config_path, "r", encoding="utf-8") as file_handler:
            yaml_object = yaml.safe_load(file_handler)
            return MetadataConfig.parse_obj(yaml_object)
