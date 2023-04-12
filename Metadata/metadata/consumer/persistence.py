"""Metadata consumer database controller."""


from pymongo.mongo_client import MongoClient

from base.aws.container_services import DATA_INGESTION_DATABASE_NAME


class InvalidMediaTypeError(Exception):
    """ Exception for invalid media type. """

    def __init__(self, media_type: str):
        super().__init__(f"Invalid media type: {media_type}")


class Persistence:  # pylint: disable=R0903
    """ Persistence class for the metadata consumer. """

    def __init__(self, db_tables: dict, client: MongoClient):
        database = client[DATA_INGESTION_DATABASE_NAME]
        self.__recordings = database[db_tables["recordings"]]

    def __aggregation_query(self, device_id: str, tenant_id: str, media_type: str):
        return [
            {
                "$match": {
                    "recording_overview.deviceID": device_id,
                    "recording_overview.tenantID": tenant_id,
                    "_media_type": media_type
                }
            }
        ]

    def __get_video_media(self, device_id, tenant_id, start, end):
        """ Get all related videos for a given video """
        result = []
        aggregation = self.__aggregation_query(device_id, tenant_id, "image")

        related = list(self.__recordings.aggregate(aggregation))

        for related_item in related:
            name_split = related_item["video_id"].split("_")
            related_time = name_split[-1]

            if (int(related_time) >= int(start) and int(related_time) <= int(end)):
                result.append(related_item["video_id"])
        return result

    def __get_snapshot_media(self, device_id, tenant_id, start):
        """ Get all related snapshots for a given video """
        result = []
        aggregation = self.__aggregation_query(device_id, tenant_id, "video")

        related = list(self.__recordings.aggregate(aggregation))

        for related_item in related:
            name_split = related_item["video_id"].split("_")
            related_start_time = name_split[-2]
            related_end_time = name_split[-1]

            if (int(start) >= int(related_start_time) and int(start) <= int(related_end_time)):
                result.append(related_item["video_id"])
        return result

    def get_video_snapshot_media(self, device_id, tenant_id, start, end, media_type):
        """ Get all related videos or snapshots for a given video or snapshot """

        if media_type == "image":
            return self.__get_snapshot_media(device_id, tenant_id, start)

        if media_type == "video":
            return self.__get_video_media(device_id, tenant_id, start, end)

        raise InvalidMediaTypeError(media_type=media_type)
