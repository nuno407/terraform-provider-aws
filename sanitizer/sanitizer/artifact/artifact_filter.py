""" artifact filter module. """
from kink import inject

from base.model.artifacts import Artifact, ImageBasedArtifact
from sanitizer.config import SanitizerConfig


@inject
class ArtifactFilter:  # pylint: disable=too-few-public-methods
    """ Artifact filter class. """

    @inject
    def is_relevant(self,
                    artifact: Artifact,
                    config: SanitizerConfig) -> bool:
        """Check if artifact is relevant."""
        return all([
            not self.__is_tenant_blacklisted(artifact, config),
            not self.__is_recorder_blacklisted(artifact, config),
            not self.__is_artifact_type_blacklisted(artifact, config)
        ])

    def __is_artifact_type_blacklisted(self,
                                       artifact: Artifact,
                                       config: SanitizerConfig) -> bool:
        """Check if artifact is from blacklisted type."""
        for artifact_type in config.type_blacklist:
            if isinstance(artifact, artifact_type):
                return True
        return False

    def __is_tenant_blacklisted(self,
                                artifact: Artifact,
                                config: SanitizerConfig) -> bool:
        """Check if artifact is from blacklisted tenant."""
        return artifact.tenant_id in set(config.tenant_blacklist)

    def __is_recorder_blacklisted(self,
                                  artifact: Artifact,
                                  config: SanitizerConfig) -> bool:
        """Check if artifact is from blacklisted recorder."""
        if not isinstance(artifact, ImageBasedArtifact):
            return False
        return artifact.recorder.value in set(config.recorder_blacklist)
