""" Artifact injector """
import logging
from typing import List

from base.model.artifacts import (IMUArtifact, MetadataArtifact, RecorderType,
                                  SignalsArtifact, Artifact)

_logger = logging.getLogger(__name__)

INJECTION_MAP = {
    RecorderType.INTERIOR: [SignalsArtifact],
    RecorderType.SNAPSHOT: [SignalsArtifact],
    RecorderType.TRAINING: [IMUArtifact, SignalsArtifact]
}


class MetadataArtifactInjector:  # pylint: disable=too-few-public-methods
    """ Metadata Artifact Injector for Video Artifacts """

    def inject(self, artifact: Artifact) -> List[MetadataArtifact]:
        """ Inject Metadata Artifact after Video Artifacts """
        injected_artifacts = []

        to_inject = INJECTION_MAP.get(artifact.recorder, [])

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
