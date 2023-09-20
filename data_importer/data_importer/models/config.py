""" Config """
import os

import yaml
from pydantic import BaseModel

from base.model.config.policy_config import PolicyConfig

TENANT_MAPPING_CONFIG_PATH = os.getenv("TENANT_MAPPING_CONFIG_PATH", None)


class DataImporterConfig(BaseModel):
    """
    Tenant configuration
    """
    policy_mapping: PolicyConfig

    @staticmethod
    def load_yaml_config(config_path: str) -> "DataImporterConfig":
        """load_yaml_config.

        Loads YAML configuration file form path

        Args:
            config_path (str): path to yaml file

        Returns:
            DataImporterConfig: configuration
        """
        with open(config_path, "r", encoding="utf-8") as file_handler:
            yaml_object = yaml.safe_load(file_handler)
            return DataImporterConfig.parse_obj(yaml_object)
