"""Sanitizer configuration module."""
import yaml
from pydantic import BaseModel, Field, validator

from base.model.artifacts import (Artifact, ImageBasedArtifact, IMUArtifact,
                                  KinesisVideoArtifact, MetadataArtifact,
                                  S3VideoArtifact, SignalsArtifact,
                                  SnapshotArtifact, VideoArtifact, PreviewSignalsArtifact)


class SanitizerConfig(BaseModel):
    """Sanitizer Configuration."""
    input_queue: str
    topic_arn: str
    message_collection: str
    db_name: str
    tenant_blacklist: list[str]
    recorder_blacklist: list[str]
    type_blacklist: set[type[Artifact]] = Field(default_factory=set)

    _artifact_types: dict[str, type[Artifact]] = {
        "Artifact": Artifact,
        "ImageBasedArtifact": ImageBasedArtifact,
        "VideoArtifact": VideoArtifact,
        "KinesisVideoArtifact": KinesisVideoArtifact,
        "S3VideoArtifact": S3VideoArtifact,
        "SnapshotArtifact": SnapshotArtifact,
        "MetadataArtifact": MetadataArtifact,
        "IMUArtifact": IMUArtifact,
        "SignalsArtifact": SignalsArtifact,
        "PreviewSignalsArtifact": PreviewSignalsArtifact,
    }

    @validator("type_blacklist", pre=True, each_item=True)
    def _validate_type_blacklist(cls, value: str) -> type[Artifact]:  # pylint: disable=no-self-argument
        if isinstance(value, str):
            return SanitizerConfig._artifact_types[value]
        return value

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
            yaml_object = yaml.safe_load(file_handler)
            return SanitizerConfig.parse_obj(yaml_object)
