"""Sanitizer configuration module."""
import yaml
from pydantic import BaseModel, Field, field_validator

from base.model.artifacts import (Artifact, CameraServiceEventArtifact,
                                  DeviceInfoEventArtifact, EventArtifact,
                                  ImageBasedArtifact, IMUArtifact,
                                  IncidentEventArtifact,
                                  MetadataArtifact, PreviewSignalsArtifact,
                                  S3VideoArtifact, SignalsArtifact,
                                  SnapshotArtifact, VideoArtifact, SOSOperatorArtifact,
                                  CameraBlockedOperatorArtifact, PeopleCountOperatorArtifact)


_artifact_types: dict[str, type[Artifact]] = {
    "Artifact": Artifact,
    "ImageBasedArtifact": ImageBasedArtifact,
    "VideoArtifact": VideoArtifact,
    "S3VideoArtifact": S3VideoArtifact,
    "SnapshotArtifact": SnapshotArtifact,
    "MetadataArtifact": MetadataArtifact,
    "IMUArtifact": IMUArtifact,
    "SignalsArtifact": SignalsArtifact,
    "PreviewSignalsArtifact": PreviewSignalsArtifact,
    "EventArtifact": EventArtifact,
    "IncidentEventArtifact": IncidentEventArtifact,
    "DeviceInfoEventArtifact": DeviceInfoEventArtifact,
    "CameraServiceEventArtifact": CameraServiceEventArtifact,
    "CameraBlockedOperatorArtifact": CameraBlockedOperatorArtifact,
    "PeopleCountOperatorArtifact": PeopleCountOperatorArtifact,
    "SOSOperatorArtifact": SOSOperatorArtifact,
}


class SanitizerConfig(BaseModel):
    """Sanitizer Configuration."""
    input_queue: str
    metadata_queue: str
    topic_arn: str
    message_collection: str
    device_info_collection: str
    db_name: str
    tenant_blacklist: list[str]
    recorder_blacklist: list[str]
    version_blacklist: dict[type[Artifact], set[str]]
    type_blacklist: set[type[Artifact]] = Field(default_factory=set)
    devcloud_raw_bucket: str
    devcloud_anonymized_bucket: str

    @field_validator("type_blacklist", mode="before")
    def _validate_type_blacklist(cls, value: set[str]) -> set[type[Artifact]]:  # pylint: disable=no-self-argument

        result: set[type[Artifact]] = set()

        for each in value:
            if each not in _artifact_types:
                raise ValueError(f"Artifact type {each} cannot be blacklisted")

            result.add(_artifact_types[each])

        return result

    @field_validator("version_blacklist", mode="before")
    def _validate_version_blacklist(cls, value: dict[str, list[str]]) -> dict[type[Artifact], set[str]]:  # pylint: disable=no-self-argument

        result: dict[type[Artifact], set[str]] = {}

        for key, val in value.items():
            if key not in _artifact_types:
                raise ValueError(f"Artifact type {key} cannot be blacklisted")

            result[_artifact_types[key]] = set(val)

        return result

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
            return SanitizerConfig.model_validate(yaml_object)
