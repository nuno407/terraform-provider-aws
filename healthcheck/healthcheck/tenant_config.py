"""Metadata config loaded from config file"""
from dataclasses import dataclass, field, fields

import yaml


@dataclass
class DatasetMappingConfig():
    """
    Config holding information about the tenant to dataset mapping
    """
    create_dataset_for: set[str] = field(default_factory=set)
    default_dataset: str = ""
    tag: str = ""


@dataclass
class TenantConfig():
    """
    Tenant configuration
    """
    dataset_mapping: DatasetMappingConfig

    @staticmethod
    def load_config_from_yaml_file(path) -> "TenantConfig":
        """Loads yaml file into a TenantConfig object. Extra yaml fields are ignored.

        Args:
            path (_type_): path of the yaml file containing the config

        Returns:
            TenantConfig: TenantConfig object containing passed yaml config
        """
        with open(path, "r", encoding="utf-8") as configfile:
            config = yaml.safe_load(configfile)

            config_objects = {}
            for attribute in fields(TenantConfig):
                # Dynamically create objects from complex types
                config_attribute_fields = [f.name for f in fields(attribute.type)]
                config_params = {key: value for key, value in
                                 config.get(attribute.name, {}).items() if
                                 key in config_attribute_fields}
                config_objects[attribute.name] = attribute.type(**config_params)

            return TenantConfig(**config_objects)
