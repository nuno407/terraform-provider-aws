"""Metadata config loaded from config file"""
from dataclasses import dataclass, field, fields

import yaml


@dataclass
class DatasetMappingConfig():
    """
    Config holding information about the tenant to dataset mapping and specific tenant config
    """
    create_dataset_for: set[str] = field(default_factory=set)
    default_dataset: str = ""
    tag: str = ""
    default_policy_document: str = ""
    policy_document_per_tenant: dict[str, str] = field(default_factory=dict)


@dataclass
class MetadataConfig():
    """
    Metadata configuration
    """
    dataset_mapping: DatasetMappingConfig

    @staticmethod
    def load_config_from_yaml_file(path) -> "MetadataConfig":
        """Loads yaml file into a MetadataConfig object. Extra yaml fields are ignored.

        Args:
            path (_type_): path of the yaml file containing the config

        Returns:
            MetadataConfig: MetadataConfig object containing passed yaml config
        """
        with open(path, "r", encoding="utf-8") as configfile:
            config = yaml.safe_load(configfile)

            config_objects = {}
            for attribute in fields(MetadataConfig):
                # Dynamically create objects from complex types
                config_attribute_fields = [f.name for f in fields(attribute.type)]
                config_params = {key: value for key, value in config.get(attribute.name, {}).items() if
                                 key in config_attribute_fields}
                config_objects[attribute.name] = attribute.type(**config_params)

            return MetadataConfig(**config_objects)
