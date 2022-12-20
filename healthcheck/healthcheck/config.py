"""Healthcheck configuration module."""
from dataclasses import dataclass, fields

import yaml


@dataclass
class HealthcheckConfig():
    """Healtheck Configuration."""
    input_queue: str
    anonymized_s3_bucket: str
    raw_s3_bucket: str
    s3_dir: str
    db_name: str
    environment_prefix: str
    tenant_blacklist: list[str]
    recorder_blacklist: list[str]

    @staticmethod
    def load_yaml_config(config_path: str) -> "HealthcheckConfig":
        """load_yaml_config.

        Loads YAML configuration file form path

        Args:
            config_path (str): path to yaml file

        Returns:
            HealthcheckConfig: configuration
        """
        with open(config_path, "r", encoding="utf-8") as file_handler:
            field_names = {f.name for f in fields(HealthcheckConfig)}
            return HealthcheckConfig(**{key: value for key,
                                        value in yaml.safe_load(file_handler).items() if key in field_names})
