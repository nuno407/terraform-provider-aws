"""Metadata config loaded from config file"""
import yaml
from pydantic import BaseModel

from base.model.config.dataset_config import DatasetConfig


class TenantConfig(BaseModel):
    """
    Tenant configuration
    """
    dataset_mapping: DatasetConfig

    @staticmethod
    def load_yaml_config(config_path: str) -> "TenantConfig":
        """load_yaml_config.

        Loads YAML configuration file form path

        Args:
            config_path (str): path to yaml file

        Returns:
            TenantConfig: configuration
        """
        with open(config_path, "r", encoding="utf-8") as file_handler:
            yaml_object = yaml.safe_load(file_handler)
            return TenantConfig.parse_obj(yaml_object)
