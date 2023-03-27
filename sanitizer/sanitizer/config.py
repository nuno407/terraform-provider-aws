"""Sanitizer configuration module."""
from dataclasses import dataclass, fields

import yaml


@dataclass
class SanitizerConfig():
    """Healtheck Configuration."""
    input_queue: str
    db_name: str
    environment_prefix: str
    tenant_blacklist: list[str]
    training_whitelist: list[str]
    recorder_blacklist: list[str]

    @staticmethod
    def load_yaml_config(config_path: str) -> "SanitizerConfig":
        """load_yaml_config.

        Loads YAML configuration file form path

        Args:
            config_path (str): path to yaml file

        Returns:
            SanitizerConfig: configuration
        """
        with open(config_path, "r", encoding="utf-8") as file_handler:
            field_names = {f.name for f in fields(SanitizerConfig)}
            return SanitizerConfig(**{key: value for key,
                                        value in yaml.safe_load(
                                            file_handler).items()
                                        if key in field_names})
