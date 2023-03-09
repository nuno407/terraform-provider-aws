from dataclasses import dataclass
from dataclasses import fields

import yaml


@dataclass
class SDRetrieverConfig():
    tenant_blacklist: list[str]
    recorder_blacklist: list[str]
    frame_buffer: int
    training_whitelist: list[str]
    request_training_upload: bool
    discard_video_already_ingested: bool

    @staticmethod
    def load_config_from_yaml_file(path) -> 'SDRetrieverConfig':
        """Loads yaml file into SDRetrieverConfig object. Extra yaml fields are ignored.

        Args:
            path (str): path of the yaml file containing the

        Returns:
            SDRetrieverConfig: SDRetrieverConfig object containing passed yaml config
        """
        with open(path, 'r') as configfile:
            # We should ignore extra fields
            field_names = set([f.name for f in fields(SDRetrieverConfig)])
            return SDRetrieverConfig(**{key: value for key,
                                        value in yaml.safe_load(configfile).items() if key in field_names})
