import logging
import re
from unittest.util import strclass
from metadata.consumer.db import Persistence


_logger = logging.getLogger('metadata_api.' + __name__)

class RelatedMediaService:

    def __init__(self, db: Persistence, s3):
        self.__db = db
        self.__s3 = s3       


    def get_related(self, tenant, deviceid, start, end, media_type):
        related = self.__db.get_video_snapshot_media(deviceid, tenant, start, end, media_type)
        return related
