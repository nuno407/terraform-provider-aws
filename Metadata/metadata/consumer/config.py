"""Metadata config loaded from config file"""
from dataclasses import Field

import yaml
from pydantic import BaseModel


class DatasetConfig(BaseModel):
    """
    Represents a list of tenants that should use a specific dataset
    """
    name: str
    tenants: list[str]


class MetadataConfig(BaseModel):
    """
    Config holding information about Metadata component
    """
    create_dataset_for: list[DatasetConfig] = []
    default_dataset: str = "Debug_Lync"
    tag: str = "RC"
    default_policy_document: str = ""
    policy_document_per_tenant: dict[str, str] = {}

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
