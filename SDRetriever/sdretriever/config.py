from dataclasses import dataclass
from dataclasses import fields
from typing import List

import yaml


@dataclass
class SDRetrieverConfig():
    tenant_blacklist: List[str]
    recorder_blacklist: List[str]
    frame_buffer: int
    training_whitelist: List[str]
    request_training_upload: bool

    @staticmethod
    def load_config_from_yaml_file(path) -> 'SDRetrieverConfig':
        """Loads yaml file into SDRetrieverConfig object. Extra yaml fields are ignored.

        Args:
            path (_type_): path of the yaml file containing the

        Returns:
            SDRetrieverConfig: SDRetrieverConfig object containing passed yaml config
        """
        with open(path, 'r') as configfile:
            # We should ignore extra fields
            field_names = set([f.name for f in fields(SDRetrieverConfig)])
            return SDRetrieverConfig(**{key: value for key,
                                        value in yaml.safe_load(configfile).items() if key in field_names})
