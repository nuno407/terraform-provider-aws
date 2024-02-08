import logging
from kink import inject
from base.mongo.engine import Engine
from artifact_api.models.mongo_models import (
    DBCameraServiceEventArtifact,
    DBDeviceInfoEventArtifact,
    DBIncidentEventArtifact)

from artifact_api.exceptions import UnknowEventArtifactException
from artifact_api.utils.imu_gap_finder import TimeRange
from base.model.artifacts import (CameraServiceEventArtifact,
                                  DeviceInfoEventArtifact, IncidentEventArtifact)

_logger = logging.getLogger(__name__)


@inject
class MongoEventService:
    def __init__(self, event_engine: Engine):
        self.__event_engine = event_engine

    async def update_events(self, range: TimeRange, tenant: str, device: str, data_type: str) -> None:
        """
        Receives a list of IMU Samples, and inserts into the timeseries database.
        Finally returns the start and end timestamp of that IMU data.

        Args:
            range (TimeRange): tuple with timestamps where IMU data starts and ends
            tenant: tentant of the device where imu data was captured
            device: device where imu data was captured
            data_type: type of data to insert
        """

        filter_query_events = {"$and": [
            {"last_shutdown.timestamp": {"$exists": False}},
            {"tenant_id": tenant},
            {"device_id": device},
            {"timestamp": {"$gte": range.min}},
            {"timestamp": {"$lte": range.max}},
        ]}

        filter_query_shutdowns = {"$and": [
            {"last_shutdown.timestamp": {"$exists": True}},
            {"last_shutdown.timestamp": {"$ne": None}},
            {"tenant_id": tenant},
            {"device_id": device},
            {"last_shutdown.timestamp": {"$gte": range.min}},
            {"last_shutdown.timestamp": {"$lte": range.max}},
        ]}

        update_query_events = {
            "$set": {
                data_type + "_available": True
            }
        }
        update_query_shutdowns = {
            "$set": {
                "last_shutdown." + data_type + "_available": True
            }
        }
        _logger.debug("Updating metadata events with filter=(%s) and update=(%s)",
                      str(filter_query_events), str(update_query_events))
        _logger.debug("Updating metadata shutdowns with filter=(%s) and update=(%s)",
                      str(filter_query_shutdowns), str(update_query_shutdowns))

        await self.__event_engine.update_many(filter_query_events, update_query_events)
        await self.__event_engine.update_many(filter_query_shutdowns, update_query_shutdowns)

    async def save_event(self, message: CameraServiceEventArtifact | DeviceInfoEventArtifact | IncidentEventArtifact):
        """
        Creates DB Event Artifact and writes the document to the mongoDB

        This is NOT an upsert, if the event already exists it will be overwritten
        Args:
            message (Union[CameraServiceEventArtifact,
                            DeviceInfoEventArtifact,
                            IncidentEventArtifact]): Event Artifact
        Raises:
            UnknowEventArtifactException:

        Returns:
            (DBCameraServiceEventArtifact,
                DBDeviceInfoEventArtifact,
                DBIncidentEventArtifact): Corresponding DB Event Artifact
        """
        if isinstance(message, CameraServiceEventArtifact):
            doc = DBCameraServiceEventArtifact.model_validate(
                message.model_dump())

        elif isinstance(message, DeviceInfoEventArtifact):
            doc = DBDeviceInfoEventArtifact.model_validate(
                message.model_dump())

        elif isinstance(message, IncidentEventArtifact):
            doc = DBIncidentEventArtifact.model_validate(
                message.model_dump())

        else:
            raise UnknowEventArtifactException()

        await self.__event_engine.save(doc)
        _logger.debug("Event saved to db [%s]", doc.model_dump_json())
        return doc
