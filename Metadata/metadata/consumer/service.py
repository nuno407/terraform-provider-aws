import logging

from metadata.consumer.db import Persistence

_logger = logging.getLogger("metadata_api." + __name__)


class RelatedMediaService:

    def __init__(self, db: Persistence):
        self.__db = db

    def get_related(self, tenant, deviceid, start, end, media_type):
        _logger.debug("getting video snapshot tenant=%s deviceid=%s start=%s end=%s media_type=%s",
                      tenant, deviceid, start, end, media_type)
        related = self.__db.get_video_snapshot_media(
            deviceid, tenant, start, end, media_type)
        return related
