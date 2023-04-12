import logging

from metadata.consumer.persistence import Persistence

_logger = logging.getLogger("metadata_api." + __name__)


class RelatedMediaService:  # pylint: disable=R0903
    """ Service class for the related media service. """

    def __init__(self, persistence: Persistence):
        self.__persistence = persistence

    def get_related(self, tenant, deviceid, start, end, media_type):
        """ Get all related videos or snapshots for a given video or snapshot """
        _logger.debug("getting video snapshot tenant=%s deviceid=%s start=%s end=%s media_type=%s",
                      tenant, deviceid, start, end, media_type)
        related = self.__persistence.get_video_snapshot_media(
            deviceid, tenant, start, end, media_type)
        return related
