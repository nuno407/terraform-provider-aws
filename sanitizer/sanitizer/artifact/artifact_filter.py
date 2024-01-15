""" artifact filter module. """
import logging
from datetime import datetime

from kink import inject

from base.model.artifacts import Artifact, ImageBasedArtifact
from sanitizer.config import SanitizerConfig
from sanitizer.device_info_db_client import DeviceInfoDBClient

_logger = logging.getLogger(__name__)


@inject
class ArtifactFilter:  # pylint: disable=too-few-public-methods
    """ Artifact filter class. """

    def __init__(self, device_info_db_client: DeviceInfoDBClient, config: SanitizerConfig):
        """Constructor"""
        self.__device_info_db_client = device_info_db_client
        self.__config = config

    def is_relevant(self, artifact: Artifact) -> bool:
        """Check if artifact is relevant."""

        return all([
            not self.__is_tenant_blacklisted(artifact),
            not self.__is_recorder_blacklisted(artifact),
            not self.__is_artifact_type_blacklisted(artifact),
            not self.__is_artifact_device_blacklisted(artifact)
        ])

    def __is_artifact_device_blacklisted(self,
                                         artifact: Artifact) -> bool:
        """
        Checks for blacklisted devices versions.
        It will fetch the device versions from a collection owned by sanitizer.
        Will also attempt to grab a "timestamp" field from the artifact, if this timestamp exists
        it will use it to fetch the latest version until that timestamp, otherwise it will fetch
        the latest until "now()"


        Args:
            artifact (Artifact): The artifact
            config (SanitizerConfig): The configuration loaded

        Returns:
            bool: True if is blacklisted, false otherwise
        """

        version_artifact_config = self.__config.version_blacklist.get(type(artifact), None)

        if not version_artifact_config:
            return False

        # Attempt to get a timestamp field from an artifact
        # otherwise the current date is used
        timestamp = datetime.now()
        if hasattr(artifact, "timestamp") and isinstance(artifact.timestamp, datetime):
            timestamp = artifact.timestamp

        device_info = self.__device_info_db_client.get_latest_device_information(
            artifact.device_id, artifact.tenant_id, timestamp)

        _logger.debug("No reference of a device version found until %s", str(timestamp))

        if device_info is None:
            return False

        _logger.debug("Latest ivscar_version=%s found until %s", device_info.ivscar_version, str(timestamp))

        if device_info.ivscar_version in version_artifact_config:
            _logger.info(
                "Device ivscar_version=%s is blacklisted for this artifact type. Ignoring it",
                device_info.ivscar_version)
            return True

        return False

    def __is_artifact_type_blacklisted(self,
                                       artifact: Artifact) -> bool:
        """Check if artifact is from blacklisted type."""
        for artifact_type in self.__config.type_blacklist:
            if isinstance(artifact, artifact_type):
                return True
        return False

    def __is_tenant_blacklisted(self,
                                artifact: Artifact) -> bool:
        """Check if artifact is from blacklisted tenant."""
        return artifact.tenant_id in set(self.__config.tenant_blacklist)

    def __is_recorder_blacklisted(self,
                                  artifact: Artifact) -> bool:
        """Check if artifact is from blacklisted recorder."""
        if not isinstance(artifact, ImageBasedArtifact):
            return False
        return artifact.recorder.value in set(self.__config.recorder_blacklist)
