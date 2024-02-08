import logging
from kink import inject
from datetime import timedelta
from base.mongo.engine import Engine
from artifact_api.models.mongo_models import (DBS3VideoArtifact, DBSnapshotArtifact, DBSnapshotUploadRule,
                                              DBVideoRecordingOverview, DBSnapshotRecordingOverview, DBVideoUploadRule)

from base.model.artifacts.upload_rule_model import SnapshotUploadRule, VideoUploadRule
from base.model.artifacts import (S3VideoArtifact, SnapshotArtifact)

_logger = logging.getLogger(__name__)


@inject
class MongoRecordingsService():

    def __init__(self, snapshot_engine: Engine, video_engine: Engine):
        """
        Constructor

        Args:
            snapshot_engine (Engine): snapshot engine
            video_engine (Engine): video engine
        """
        self.__snapshot_engine = snapshot_engine
        self.__video_engine = video_engine

    async def update_videos_correlations(self, correlated_videos: list[str], snapshot_id: str):
        """_summary_
        """
        update_video = [
            {
                "$addFields": {
                    "recording_overview.snapshots_paths": {
                        "$setUnion": [
                            "$recording_overview.snapshots_paths",
                            [snapshot_id]
                        ]
                    }
                }
            },
            {
                "$set": {
                    "recording_overview.#snapshots": {
                        "$size": "$recording_overview.snapshots_paths"
                    }
                }
            }
        ]

        filter_correlated = {
            "video_id": {"$in": correlated_videos}
        }

        _logger.debug("Updating snapshot correlations for %s",
                      correlated_videos)
        await self.__video_engine.update_many(filter_correlated, update_video)

    async def update_snapshots_correlations(self, correlated_snapshots: list[str], video_id: str):
        """_summary_
        """
        update_snapshot = {
            "$addToSet": {
                "recording_overview.source_videos": video_id
            }
        }

        filter_correlated = {
            "video_id": {"$in": correlated_snapshots}
        }

        _logger.debug("Updating video correlations for %s",
                      correlated_snapshots)
        await self.__snapshot_engine.update_many(filter_correlated, update_snapshot)

    async def upsert_snapshot(self, message: SnapshotArtifact, correlated_ids: list[str]):
        """_summary_

        Args:
            message (SnapshotArtifact): _description_
        """
        rec_overview = DBSnapshotRecordingOverview(
            devcloud_id=message.devcloudid,
            device_id=message.device_id,
            tenant_id=message.tenant_id,
            recording_time=message.timestamp,
            source_videos=correlated_ids
        )

        doc = DBSnapshotArtifact(
            video_id=message.artifact_id,
            filepath=message.s3_path,
            recording_overview=rec_overview
        )

        # In our DB, videos can be uniquely identified by video_id and _media_type
        await self.__snapshot_engine.update_one_flatten(
            query={
                "video_id": doc.video_id,
                "_media_type": doc.media_type
            },
            set_command=self.__snapshot_engine.dump_model(doc),
            upsert=True
        )
        _logger.debug("Snapshot upserted to db [%s]", doc.model_dump_json())

    async def upsert_video_aggregated_metadata(self, aggregated_metadata: dict[str, str | int | float | bool], correlated_id: str):

        recording_overview = DBVideoRecordingOverview.model_validate({"aggregated_metadata":aggregated_metadata})
        video_model = DBS3VideoArtifact(video_id=correlated_id,recording_overview=recording_overview)

        video_model.mdf_available = "Yes"

        await self.__video_engine.update_one_flatten(
            query={
                "video_id": correlated_id,
                "_media_type": "video"
            },
            set_command=self.__video_engine.dump_model(video_model),
            upsert=True
        )

    async def upsert_video(self, message: S3VideoArtifact, correlated_ids: list[str]):
        """_summary_
        Args:
            message (S3VideoArtifact): _description_
        """
        resolution_str = f"{message.resolution.width}x{message.resolution.height}"
        time_str = message.timestamp.strftime("%Y-%m-%d %H:%M:%S")

        duration = timedelta(seconds=message.actual_duration)
        hours, remainder = divmod(duration.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)

        overview = DBVideoRecordingOverview(snapshots=len(correlated_ids),
                                            devcloud_id=message.devcloudid,
                                            device_id=message.device_id,
                                            length=f"{hours:01}:{minutes:02}:{seconds:02}",
                                            recording_time=message.timestamp,
                                            recording_duration=message.actual_duration,
                                            tenant_id=message.tenant_id,
                                            time=time_str,
                                            snapshots_paths=correlated_ids)

        doc = DBS3VideoArtifact(video_id=message.artifact_id,
                                MDF_available="No",
                                filepath=message.s3_path,
                                resolution=resolution_str,
                                recording_overview=overview
                                )

        # In our DB, videos can be uniquely identified by video_id and _media_type
        await self.__video_engine.update_one_flatten(
            query={
                "video_id": doc.video_id,
                "_media_type": doc.media_type
            },
            set_command=self.__video_engine.dump_model(doc),
            upsert=True
        )
        _logger.debug("Video upserted to db [%s]", doc.model_dump_json())

    async def get_correlated_videos_for_snapshot(self, message: SnapshotArtifact) -> list[DBSnapshotArtifact]:
        """_summary_

        Args:
            message (SnapshotArtifact): _description_
        """

        correlated = {
            "recording_overview.deviceID": message.device_id,
            "_media_type": "video",
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

        return [_ async for _ in self.__video_engine.find(correlated)]

    async def get_correlated_snapshots_for_video(self, message: S3VideoArtifact) -> list[DBSnapshotArtifact]:

        """_summary_

        Args:
            message (S3VideoArtifact): _description_
        """

        correlated = {
            "recording_overview.deviceID": message.device_id,
            "_media_type": "image",
            "recording_overview.recording_time": {
                "$gte": message.timestamp,
                "$lte": message.end_timestamp
            }
        }

        return [_ async for _ in self.__snapshot_engine.find(correlated)]

    async def attach_rule_to_video(self, message: VideoUploadRule) -> None:
        """ Attaches the upload rule to the given video. """
        doc = DBVideoUploadRule(
            name=message.rule.rule_name,
            version=message.rule.rule_version,
            footage_from=message.footage_from,
            footage_to=message.footage_to,
            origin=message.rule.origin
        )

        # In our DB, videos can be uniquely identified by video_id and _media_type
        await self.__video_engine.update_one(
            query={
                "video_id": message.video_id,
                "_media_type": "video"
            },
            command={
                # in case that the video is not created we add the missing fields
                "$setOnInsert": {
                    "video_id": message.video_id,
                    "_media_type": "video"
                },
                # Add rule to upload_rule list
                "$addToSet": {
                    "upload_rules": self.__video_engine.dump_model(doc),
                }
            },
            upsert=True
        )

    async def attach_rule_to_snapshot(self, message: SnapshotUploadRule) -> None:
        """ Attaches the upload rule to the given snapshot. """
        doc = DBSnapshotUploadRule(
            name=message.rule.rule_name,
            version=message.rule.rule_version,
            snapshot_timestamp=message.snapshot_timestamp,
            origin=message.rule.origin
        )

        # In our DB, videos can be uniquely identified by video_id and _media_type
        await self.__snapshot_engine.update_one(
            query={
                "video_id": message.snapshot_id,
                "_media_type": "image"
            },
            command={
                # in case that the snapshot is not created we add the missing fields
                "$setOnInsert": {
                    "video_id": message.snapshot_id,
                    "_media_type": "image"
                },
                # Add rule to upload_rule list
                "$addToSet": {"upload_rules": self.__snapshot_engine.dump_model(doc)}
            },
            upsert=True
        )
