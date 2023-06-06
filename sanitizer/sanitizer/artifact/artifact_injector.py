""" Artifact injector """
import logging
from typing import List

from base.model.artifacts import (Artifact, IMUArtifact,
                                  PreviewSignalsArtifact, RecorderType,
                                  SignalsArtifact, MultiSnapshotArtifact)

_logger = logging.getLogger(__name__)

INJECTION_MAP = {
    RecorderType.INTERIOR_PREVIEW: [PreviewSignalsArtifact],
    RecorderType.INTERIOR: [SignalsArtifact],
    RecorderType.SNAPSHOT: [SignalsArtifact],
    RecorderType.TRAINING: [IMUArtifact, SignalsArtifact]
}


class MetadataArtifactInjector:  # pylint: disable=too-few-public-methods
    """ Metadata Artifact Injector for Video Artifacts """

    def inject(self, artifact: Artifact) -> List[Artifact]:
        """ Inject Metadata Artifact after Video Artifacts """
        injected_artifacts = []

        to_inject = INJECTION_MAP.get(artifact.recorder, [])

        if PreviewSignalsArtifact in to_inject and isinstance(artifact, MultiSnapshotArtifact):
            preview_metadata_artifact = PreviewSignalsArtifact(  # pylint: disable=unexpected-keyword-arg
                tenant_id=artifact.tenant_id,
                device_id=artifact.device_id,
                timestamp=artifact.timestamp,
                end_timestamp=artifact.end_timestamp,
                referred_artifact=artifact)
            injected_artifacts.append(preview_metadata_artifact)
            _logger.info("injecting preview metadata artifact: %s", preview_metadata_artifact)

        if SignalsArtifact in to_inject:
            signals_artifact = SignalsArtifact(  # pylint: disable=unexpected-keyword-arg
                tenant_id=artifact.tenant_id,
                device_id=artifact.device_id,
                referred_artifact=artifact)
            injected_artifacts.append(signals_artifact)
            _logger.info("injecting signals artifact: %s", signals_artifact)

        if IMUArtifact in to_inject:
            imu_artifact = IMUArtifact(  # pylint: disable=unexpected-keyword-arg
                tenant_id=artifact.tenant_id,
                device_id=artifact.device_id,
                referred_artifact=artifact)
            injected_artifacts.append(imu_artifact)
            _logger.info("injecting imu artifact: %s", imu_artifact)

        return injected_artifacts
