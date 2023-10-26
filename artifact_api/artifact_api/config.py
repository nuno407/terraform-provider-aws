"""ArtifactAPI config loaded from config file"""

import yaml
from base.model.base_model import ConfiguredBaseModel
from pydantic import Field

class ArtifactAPIConfig(ConfiguredBaseModel):
    """
    Config holding information about ArtifactAPI component
    """
    mongodb_name: str = Field(..., alias="mongodb-name")
    voxeldb_name: str = Field(..., alias="voxeldb-name")

    @staticmethod
    def load_yaml_config(config_path: str) -> "ArtifactAPIConfig":
        """load_yaml_config.

        Loads YAML configuration file form path

        Args:
            config_path (str): path to yaml file

        Returns:
            VoxelConfig: configuration
        """
        with open(config_path, "r", encoding="utf-8") as file_handler:
            yaml_object = yaml.safe_load(file_handler)
            return ArtifactAPIConfig.model_validate(yaml_object)
