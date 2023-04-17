""" artifact filter module. """
from kink import inject

from base.model.artifacts import Artifact
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
            not self.__is_recorder_blacklisted(artifact, config)
        ])

    def __is_tenant_blacklisted(self,
                                artifact: Artifact,
                                config: SanitizerConfig) -> bool:
        """Check if artifact is from blacklisted tenant."""
        return artifact.tenant_id in set(config.tenant_blacklist)

    def __is_recorder_blacklisted(self,
                                  artifact: Artifact,
                                  config: SanitizerConfig) -> bool:
        """Check if artifact is from blacklisted recorder."""
        return artifact.recorder.value in set(config.recorder_blacklist)
