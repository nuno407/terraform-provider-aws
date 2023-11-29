"""Downloader configuration module."""
import yaml
from pydantic import AnyHttpUrl, BaseModel


class ArtifactDownloaderConfig(BaseModel):
    """Downloader Configuration."""
    input_queue: str
    artifact_base_url: AnyHttpUrl
    raw_bucket: str  # This is only used for the temporary conversion of messages to pydantic models

    @staticmethod
    def load_yaml_config(config_path: str) -> "ArtifactDownloaderConfig":
        """load_yaml_config.

        Loads YAML configuration file form path

        Args:
            config_path (str): path to yaml file

        Returns:
             ArtifactDownloaderConfig: configuration
        """
        with open(config_path, "r", encoding="utf-8") as file_handler:
            yaml_object = yaml.safe_load(file_handler)
            return ArtifactDownloaderConfig.model_validate(yaml_object)
