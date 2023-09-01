""" sdr config module """
from pydantic import BaseModel

import yaml


class SelectorConfig(BaseModel):  # pylint: disable=too-many-instance-attributes, too-few-public-methods
    """Selector configuration"""
    max_GB_per_device_per_month: int
    total_GB_per_month: int
    upload_window_seconds_start: int
    upload_window_seconds_end: int

    @staticmethod
    def load_config_from_yaml_file(path) -> "SelectorConfig":
        """Loads yaml file into SelectorConfig object. Extra yaml fields are ignored.

        Args:
            path (str): path of the yaml file containing the config

        Returns:
            SelectorConfig: SelectorConfig object containing passed yaml config
        """
        with open(path, "r", encoding="utf-8") as configfile:
            yaml_object = yaml.safe_load(configfile)
            return SelectorConfig.parse_obj(yaml_object)
