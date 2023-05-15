"""Artifact adapter"""
from typing import Optional

from base.model.artifacts import IMUArtifact, MetadataArtifact, SignalsArtifact

from mdfparser.constants import DataType
from mdfparser.exceptions import InvalidMessage
from mdfparser.interfaces.input_message import InputMessage


class ArtifactAdapter:  # pylint: disable=too-few-public-methods
    """Artifact Adapter Class"""

    def adapt_message(self, artifact: MetadataArtifact) -> InputMessage:
        """
        Convert a MetadataArtifact to an InputMessage.

        Args:
            artifact (MetadataArtifact): A MetadataArtifact sent by the SDRetriever.

        Raises:
            InvalidMessage: If the artifact type is uknown.

        Returns:
            InputMessage: The converted input message.
        """
        artifact_type: Optional[DataType] = None

        if isinstance(artifact, IMUArtifact):
            artifact_type = DataType.IMU
        if isinstance(artifact, SignalsArtifact):
            artifact_type = DataType.METADATA

        if artifact_type is None:
            raise InvalidMessage("Artifact type is neither IMU nor Metadata")

        converted_recorder: str = artifact.referred_artifact.recorder.value.replace(
            "Recorder", "")

        return InputMessage(artifact.referred_artifact.artifact_id,
                            s3_path=artifact.s3_path,
                            data_type=artifact_type,
                            tenant=artifact.tenant_id,
                            device_id=artifact.device_id,
                            recorder=converted_recorder
                            )
