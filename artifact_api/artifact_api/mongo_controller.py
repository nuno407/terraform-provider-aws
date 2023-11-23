"""mongo controller service module"""
from typing import Union
from datetime import timedelta
from logging import Logger
from base.aws.container_services import ContainerServices
from base.model.metadata.api_messages import IMUDataArtifact, IMUSample
from base.model.artifacts import (CameraBlockedOperatorArtifact, CameraServiceEventArtifact,
                                  DeviceInfoEventArtifact, IncidentEventArtifact, IMUArtifact,
                                  PeopleCountOperatorArtifact, S3VideoArtifact, SnapshotArtifact,
                                  SOSOperatorArtifact)
from base.mongo.engine import Engine
from kink import inject
from artifact_api.models.mongo_models import (DBCameraServiceEventArtifact,
                                              DBDeviceInfoEventArtifact, DBIncidentEventArtifact, DBIMUSample,
                                              DBS3VideoArtifact, DBSnapshotArtifact,
                                              DBVideoRecordingOverview, DBSnapshotRecordingOverview)
from artifact_api.exceptions import IMUEmptyException, UnknowEventArtifactException, InvalidOperatorArtifactException
from artifact_api.utils.imu_gap_finder import IMUGapFinder, TimeRange


_logger: Logger = ContainerServices.configure_logging("artifact_api")


@inject()
class MongoController:  # pylint:disable=too-many-arguments
    """
    Mongo Controller Class
    """

    def __init__(self, event_engine: Engine, operator_feedback_engine: Engine,
                 processed_imu_engine: Engine, snapshot_engine: Engine,
                 video_engine: Engine, imu_gap_finder: IMUGapFinder) -> None:

        self.__event_engine = event_engine
        self.__operator_feedback_engine = operator_feedback_engine
        self.__snapshot_engine = snapshot_engine
        self.__video_engine = video_engine
        self.__processed_imu_engine = processed_imu_engine
        self.__imu_gap_finder = imu_gap_finder

    async def update_videos_correlations(self, correlated_videos: list[str], snapshot_id: str):
        """_summary_
        """
        update_video = {
            "$push": {
                "recording_overview.snapshots_paths": snapshot_id
            },
            "$inc": {
                "#snapshots": 1
            }
        }

        filter_correlated = {
            "video_id": {"$in": correlated_videos}
        }

        await self.__video_engine.update_many(filter_correlated, update_video)

    async def update_snapshots_correlations(self, correlated_snapshots: list[str], video_id: str):
        """_summary_
        """
        update_snapshot = {
            "$push": {
                "recording_overview.source_videos": video_id
            }
        }

        filter_correlated = {
            "video_id": {"$in": correlated_snapshots}
        }

        await self.__snapshot_engine.update_many(filter_correlated, update_snapshot)

    async def create_snapshot(self, message: SnapshotArtifact):
        """_summary_

        Args:
            message (SnapshotArtifact): _description_
        """
        rec_overview = DBSnapshotRecordingOverview(
            devcloud_id=message.devcloudid,
            device_id=message.device_id,
            tenant_id=message.tenant_id,
            recording_time=message.timestamp,
            source_videos=message.correlated_artifacts
        )

        doc = DBSnapshotArtifact(
            video_id=message.artifact_id,
            filepath=message.s3_path,
            recording_overview=rec_overview
        )

        await self.__snapshot_engine.save(doc)

        return doc

    async def create_video(self, message: S3VideoArtifact):
        """_summary_
        Args:
            message (S3VideoArtifact): _description_
        """
        resolution_str = f"{message.resolution.width}x{message.resolution.height}"
        time_str = message.timestamp.strftime("%Y-%m-%d %H:%M:%S")

        duration = timedelta(seconds=message.actual_duration)
        hours, remainder = divmod(duration.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)

        overview = DBVideoRecordingOverview(snapshots=0,
                                            devcloud_id=message.devcloudid,
                                            device_id=message.device_id,
                                            length=f"{hours:01}:{minutes:02}:{seconds:02}",
                                            recording_time=message.timestamp,
                                            recording_duration=message.actual_duration,
                                            snapshots_paths=message.correlated_artifacts,
                                            tenant_id=message.tenant_id,
                                            time=time_str)

        doc = DBS3VideoArtifact(video_id=message.artifact_id,
                                MDF_available="No",
                                media_type="video",
                                filepath=message.s3_path,
                                resolution=resolution_str,
                                recording_overview=overview
                                )

        await self.__video_engine.save(doc)
        return doc

    async def get_correlated_videos_for_snapshot(self, message: SnapshotArtifact):
        """_summary_

        Args:
            message (SnapshotArtifact): _description_
        """

        correlated = {
            "recording_overview.deviceID": message.device_id,
            "recording_overview.recording_time": {"$lte": message.timestamp},
            "$expr": {
                "$gte": [
                    {"$add": [
                        "$recording_overview.recording_time",
                        {"$multiply":
                             ["$recording_overview.recording_duration", 1000]
                         }
                    ]},
                    message.timestamp
                ]
            }
        }

        correlated_artifacts = self.__video_engine.find(correlated)

        correlated_artifacts_ids = [cor.filepath async for cor in correlated_artifacts]

        return correlated_artifacts_ids

    async def get_correlated_snapshots_for_video(self, message: S3VideoArtifact):

        """_summary_

        Args:
            message (S3VideoArtifact): _description_
        """

        correlated = {
            "deviceID": message.device_id,
            "recording_overview.recording_time": {
                "$gte": message.timestamp,
                "$lte": message.end_timestamp
            }
        }

        correlated_artifacts = self.__snapshot_engine.find(correlated)

        correlated_artifacts_ids = [cor.filepath async for cor in correlated_artifacts]

        return correlated_artifacts_ids

    async def create_event(self, message: Union[CameraServiceEventArtifact,
                                                DeviceInfoEventArtifact,
                                                IncidentEventArtifact]):
        """
        Creates DB Event Artifact and writes the document to the mongoDB
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
            doc = DBCameraServiceEventArtifact(**message.model_dump())

        elif isinstance(message, DeviceInfoEventArtifact):
            doc = DBDeviceInfoEventArtifact(**message.model_dump())

        elif isinstance(message, IncidentEventArtifact):
            doc = DBIncidentEventArtifact(**message.model_dump())

        else:
            raise UnknowEventArtifactException()

        await self.__event_engine.save(doc)
        return doc

    # Operator Events
    async def create_operator_feedback_event(self, artifact: Union[SOSOperatorArtifact,
                                                                   PeopleCountOperatorArtifact,
                                                                   CameraBlockedOperatorArtifact]):
        """
        Create operator feedback entry in database
        Args:
            artifact: The artifact to store
        """

        if isinstance(artifact, (SOSOperatorArtifact, PeopleCountOperatorArtifact, CameraBlockedOperatorArtifact)):
            await self.__operator_feedback_engine.save(artifact)
        else:
            raise InvalidOperatorArtifactException()

    async def _update_events(self, imu_range: TimeRange, imu_tenant, imu_device, data_type: str) -> None:
        """
        Receives a list of IMU Samples, and inserts into the timeseries database.
        Finally returns the start and end timestamp of that IMU data.

        Args:
            imu_range (TimeRange): tuple with timestamps where IMU data starts and ends
            imu_tenant: tentant of the device where imu data was captured
            imu_device: device where imu data was captured
            data_type: type of data to insert
        """

        filter_query_events = {"$and": [
            {"last_shutdown.timestamp": {"$exists": False}},
            {"tenant_id": imu_tenant},
            {"device_id": imu_device},
            {"timestamp": {"$gte": imu_range.min}},
            {"timestamp": {"$lte": imu_range.max}},
        ]}

        filter_query_shutdowns = {"$and": [
            {"last_shutdown.timestamp": {"$exists": True}},
            {"last_shutdown.timestamp": {"$ne": None}},
            {"tenant_id": imu_tenant},
            {"device_id": imu_device},
            {"last_shutdown.timestamp": {"$gte": imu_range.min}},
            {"last_shutdown.timestamp": {"$lte": imu_range.max}},
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

        await self.__event_engine.update_many(filter=filter_query_events, update=update_query_events)
        await self.__event_engine.update_many(filter=filter_query_shutdowns, update=update_query_shutdowns)

    async def _insert_mdf_imu_data(self, imu_data: list[IMUSample]) -> tuple[list[TimeRange], str, str]:
        """
        Receives a list of IMU Samples, and inserts into the timeseries database.
        Finally returns the start and end timestamp of that IMU data.

        Args:
            imu_data: _description_
            imu_gap_finder: _description_
        """

        if len(imu_data) == 0:
            _logger.warning("The imu sample list does not contain any information")
            raise IMUEmptyException()

        imu_list: list = [doc.model_dump() for doc in imu_data]

        await self.__processed_imu_engine.save_all([DBIMUSample.model_validate(imu) for imu in imu_list])

        _logger.info("IMU data was inserted into mongodb")
        return self.__imu_gap_finder.get_valid_imu_time_ranges(
            imu_list), imu_data[0].source.tenant, imu_data[0].source.device_id

    async def process_imu_artifact(self, imu_data_artifact: IMUDataArtifact):
        """_summary_

        Args:
            imu_data_artifact (IMUDataArtifact): _description_
        """

        imu_data: list[IMUSample] = imu_data_artifact.data
        imu_artifact: IMUArtifact = imu_data_artifact.message
        _logger.debug("Inserting IMU data from artifact: %s", str(imu_artifact))
        imu_ranges, imu_tenant, imu_device = await self._insert_mdf_imu_data(imu_data)
        for imu_range in imu_ranges:
            await self._update_events(imu_range, imu_tenant, imu_device, "imu")
