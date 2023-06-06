""" sdr config module """
from dataclasses import dataclass, fields

import yaml


@dataclass
class SDRetrieverConfig:  # pylint: disable=too-many-instance-attributes
    """ SDR configuration """
    tenant_blacklist: list[str]
    recorder_blacklist: list[str]
    frame_buffer: int
    training_whitelist: list[str]
    request_training_upload: bool
    discard_video_already_ingested: bool
    ingest_from_kinesis: bool
    temporary_bucket: str
    input_queue: str

    @staticmethod
    def load_config_from_yaml_file(path) -> "SDRetrieverConfig":
        """Loads yaml file into SDRetrieverConfig object. Extra yaml fields are ignored.

        Args:
            path (str): path of the yaml file containing the

        Returns:
            SDRetrieverConfig: SDRetrieverConfig object containing passed yaml config
        """
        with open(path, "r", encoding="utf-8") as configfile:
            # We should ignore extra fields
            field_names = set(f.name for f in fields(SDRetrieverConfig))
            return SDRetrieverConfig(**{key: value for key,
                                        value in yaml.safe_load(configfile).items()
                                        if key in field_names})
