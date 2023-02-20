"""Metadata consumer database controller."""
from typing import Dict

from pymongo.mongo_client import MongoClient

from base.aws.container_services import DATA_INGESTION_DATABASE_NAME


class Persistence:

    def __init__(self, connection_string: str, db_tables: Dict, client: MongoClient = None):
        # for unit testing allow a mongomock client to be injected with the last parameter
        if client is None:
            client = MongoClient(connection_string)
        db_client = client[DATA_INGESTION_DATABASE_NAME]
        self.__recordings = db_client[db_tables["recordings"]]

    def get_video_snapshot_media(self, deviceID, tenantID, start, end, media_type):
        result = []
        # If end is 0 it's a snapshot, get all videos
        if media_type == "image":
            aggregation = [{"$match": {
                "recording_overview.deviceID": deviceID,
                "recording_overview.tenantID": tenantID,
                "_media_type": "video"}}]
        # Else it's a video, get all snapshots
        elif media_type == "video":
            # all snapshots for that tennant and device
            aggregation = [{"$match": {"recording_overview.deviceID": deviceID,
                                       "recording_overview.tenantID": tenantID, "_media_type": "image"}}]

        related = list(self.__recordings.aggregate(aggregation))

        if media_type == "image":
            for related_item in related:
                name_split = related_item["video_id"].split("_")
                related_start_time = name_split[-2]
                related_end_time = name_split[-1]

                if (int(start) >= int(related_start_time) and int(start) <= int(related_end_time)):
                    result.append(related_item["video_id"])
        elif media_type == "video":
            for related_item in related:
                name_split = related_item["video_id"].split("_")
                related_time = name_split[-1]

                if (int(related_time) >= int(start) and int(related_time) <= int(end)):
                    result.append(related_item["video_id"])

        return result
