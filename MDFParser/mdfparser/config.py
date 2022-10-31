from dataclasses import dataclass, fields
import yaml


@dataclass
class MdfParserConfig():
    input_queue: str
    metadata_output_queue: str

    @staticmethod
    def load_config_from_yaml_file(path) -> 'MdfParserConfig':
        """Loads yaml file into MdfParserConfig object. Extra yaml fields are ignored.

        Args:
            path (_type_): path of the yaml file containing the config.

        Returns:
            MdfParserConfig: MdfParserConfig object containing passed yaml config
        """
        with open(path, 'r') as configfile:
            # We should ignore extra fields
            field_names = set([f.name for f in fields(MdfParserConfig)])
            return MdfParserConfig(**{key: value for key,
                                      value in yaml.safe_load(configfile).items() if key in field_names})
