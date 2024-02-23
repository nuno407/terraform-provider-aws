"""Tests for mongodb controller"""

from datetime import datetime, timezone
from typing import Union
from unittest.mock import ANY, AsyncMock, MagicMock, Mock
from pytest import fixture, mark
from base.model.artifacts import (CameraServiceEventArtifact, DeviceInfoEventArtifact, IncidentEventArtifact,
                                  RecorderType, Recording, Resolution, S3VideoArtifact, SnapshotArtifact,
                                  TimeWindow)
from base.model.event_types import (CameraServiceState, EventType, GeneralServiceState, IncidentType,
                                    Shutdown, ShutdownReason)
from artifact_api.mongo.mongo_service import MongoService
from artifact_api.mongo.services.mongo_recordings_service import MongoRecordingsService
from artifact_api.mongo.services.mongo_signals_service import MongoSignalsService


@mark.unit
class TestMongoController:  # pylint: disable=duplicate-code
    """Class with tests for mongodb controller"""

    @fixture
    def correlated_videos(self) -> list[str]:
        """Fixture for correlated videos"""
        return ["correlated_1", "correlated_2"]

    @fixture
    def correlated_snapshots(self) -> list[str]:
        """Fixture for correlated snapshots"""
        return ["correlated_1", "correlated_2"]

    @fixture
    def snapshot_id(self) -> str:
        """Fixture for snapshot id"""
        return "snapshot_id_1"

    @fixture
    def video_id(self) -> str:
        """Fixture for video id"""
        return "video_id_1"

    @fixture
    def video(self) -> S3VideoArtifact:
        """Fixture for video artifact"""
        return S3VideoArtifact(
            artifact_id="bar",
            raw_s3_path="s3://raw/foo/bar.something",
            anonymized_s3_path="s3://anonymized/foo/bar.something",
            tenant_id="foo",
            device_id="bar",
            resolution=Resolution(width=1920, height=1080),
            recorder=RecorderType.INTERIOR,
            timestamp=datetime.fromisoformat("2023-04-13T07:14:15.774082+00:00"),
            end_timestamp=datetime.fromisoformat("2023-04-13T07:15:15.774082+00:00"),
            actual_duration=123,
            upload_timing=TimeWindow(
                start=datetime.fromisoformat("2023-04-13T08:00:00+00:00"),
                end=datetime.fromisoformat("2023-04-13T08:01:00+00:00")),
            footage_id="my_footage_id",
            rcc_s3_path="s3://dev-bucket/key",
            s3_path="s3://dev-bucket/key",
            recordings=[Recording(recording_id="TrainingRecorder-abc", chunk_ids=[1, 2, 3])]
        )

    @fixture
    def snapshot_artifact(self) -> SnapshotArtifact:
        """Fixture for snapshot artifact"""
        return SnapshotArtifact(
            artifact_id="bar",
            raw_s3_path="s3://raw/foo/bar.something",
            anonymized_s3_path="s3://anonymized/foo/bar.something",
            tenant_id="foo",
            device_id="bar",
            s3_path="s3://dev-bucket/key",
            recorder=RecorderType.SNAPSHOT,
            timestamp=datetime.fromisoformat("2023-04-13T07:14:15.770982+00:00"),
            end_timestamp=datetime.fromisoformat("2023-04-13T07:14:15.770982+00:00"),
            upload_timing=TimeWindow(
                start="2023-04-13T08:00:00+00:00",  # type: ignore
                end="2023-04-13T08:01:00+00:00"),  # type: ignore
            uuid="abc"
        )

    @fixture
    def camera_event(self) -> CameraServiceEventArtifact:
        """Fixture for camera event artifact"""
        return CameraServiceEventArtifact(
            tenant_id="mock_tenant_id_foo",
            device_id="mock_device_id_bar",
            timestamp=datetime(2023, 1, 2, tzinfo=timezone.utc),
            event_name=EventType.CAMERA_SERVICE,
            artifact_name="camera_service",
            service_state=GeneralServiceState.UNKNOWN,
            camera_name="mock_camera_name",
            camera_state=[CameraServiceState.UNKNOWN, CameraServiceState.CAMERA_READY]
        )

    @fixture
    def device_info_event(self) -> DeviceInfoEventArtifact:
        """Fixture for device info event artifact"""
        return DeviceInfoEventArtifact(
            tenant_id="mock_tenant_id_foo",
            device_id="mock_device_id_bar",
            timestamp=datetime(2023, 1, 2, tzinfo=timezone.utc),
            event_name=EventType.DEVICE_INFO,
            system_report="mock_report",
            software_versions=[{"version": "1.0", "software_name": "app"}],
            device_type="mock_type",
            last_shutdown=Shutdown(reason=ShutdownReason.UNKNOWN, reason_description=None,
                                   timestamp=datetime(2023, 1, 1, tzinfo=timezone.utc)),
        )

    @fixture
    def incident_event(self) -> IncidentEventArtifact:
        """Fixture for incident event artifact"""
        return IncidentEventArtifact(
            tenant_id="mock_tenant_id_foo",
            device_id="mock_device_id_bar",
            timestamp=datetime(2023, 1, 2, tzinfo=timezone.utc),
            event_name=EventType.INCIDENT,
            incident_type=IncidentType.UNKNOWN,
            bundle_id="mock_bundle_id",
        )

    async def test_upsert_snapshot(self, snapshot_engine: MagicMock, mongo_controller: MongoService,
                                   snapshot_artifact: SnapshotArtifact, mongo_recordings_controller: MongoRecordingsService):
        """Test for upsert_snapshot method

        Args:
            snapshot_engine (MagicMock): snapshot engine mock
            mongo_controller (MongoController): class where the tested method is defined
            snapshot_artifact (SnapshotArtifact): snapshot artifact to be ingested
        """
        correlated_ids = ["mock_id"]
        mongo_recordings_controller.update_snapshots_correlations = AsyncMock()
        snapshot_engine.update_one_flatten = AsyncMock()

        await mongo_controller.upsert_snapshot(snapshot_artifact, correlated_ids)
        snapshot_engine.update_one_flatten.assert_called_once_with(query={
            "video_id": snapshot_artifact.artifact_id,
            "_media_type": "image"
        },
            set_command=ANY,
            upsert=True
        )
        mongo_recordings_controller.update_snapshots_correlations.assert_called_once_with(
            [snapshot_artifact.artifact_id], correlated_ids)

    async def test_update_videos_correlations(self, correlated_videos: list[str], snapshot_id: str,
                                              video_engine: MagicMock, mongo_controller: MongoService):
        """Test for update_videos_correlations method

        Args:
            correlated_videos (list[str]): list of correlated video ids
            snapshot_id (str): id of snapshot
            video_engine (MagicMock): video engine mock
            mongo_controller (MongoController): class where the tested method is defined
        """

        video_engine.update_many = AsyncMock()
        update_video = update_video = [{
            "$addFields": {
                "recording_overview.snapshots_paths": {
                    "$setUnion": [
                        {"$ifNull":["$recording_overview.snapshots_paths",[]]},
                        [snapshot_id],
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

        await mongo_controller.update_videos_correlations(correlated_videos, snapshot_id)
        video_engine.update_many.assert_called_once_with(filter_correlated, update_video)

    async def test_get_correlated_videos_for_snapshot(self, video_engine: MagicMock,
                                                      mongo_controller: MongoService):
        """Test for get_correlated_videos_for_snapshot method

        Args:
            video_engine (MagicMock): video engine mock
            mongo_controller (MongoController): class where the tested method is defined
        """

        message = Mock(device_id="mock_device_id_bar", timestamp=111112, end_timestamp=111113)

        correlated = {
            "recording_overview.deviceID": message.device_id,
            "recording_overview.recording_time": {"$lte": message.timestamp},
            "_media_type": "video",
            "$expr": {
                "$gte": [
                    {"$add": [
                        "$recording_overview.recording_time",
                        {"$multiply": ["$recording_overview.recording_duration", 1000]}
                    ]},
                    message.timestamp
                ]
            }
        }
        result = [Mock(), Mock()]

        correlated_artifacts = MagicMock()
        correlated_artifacts.__aiter__.return_value = [Mock(), Mock()]

        video_engine.find = MagicMock()
        video_engine.find.return_value = correlated_artifacts

        result = await mongo_controller.get_correlated_videos_for_snapshot(message)

        video_engine.find.assert_called_once_with(correlated)
        assert result == correlated_artifacts.__aiter__.return_value

    async def test_get_correlated_snapshots_for_video(self, snapshot_engine: MagicMock,
                                                      mongo_controller: MongoService):
        """Test for get_correlated_snapshots_for_video method

        Args:
            snapshot_engine (MagicMock): snapshot engine mock
            mongo_controller (MongoController): class where the tested method is defined
        """

        message = Mock(device_id="mock_device_id_bar", timestamp=111112, end_timestamp=111113)

        correlated = {
            "recording_overview.deviceID": message.device_id,
            "_media_type": "image",
            "recording_overview.recording_time": {
                "$gte": message.timestamp,
                "$lte": message.end_timestamp
            }
        }

        correlated_artifacts = MagicMock()
        correlated_artifacts.__aiter__.return_value = [Mock(), Mock()]

        snapshot_engine.find = MagicMock()
        snapshot_engine.find.return_value = correlated_artifacts

        result = await mongo_controller.get_correlated_snapshots_for_video(message)
        snapshot_engine.find.assert_called_once_with(correlated)

        assert result == correlated_artifacts.__aiter__.return_value

    @mark.parametrize("message", [
        IncidentEventArtifact(
            tenant_id="mock_tenant_id_foo",
            device_id="mock_device_id_bar",
            timestamp=datetime(2023, 1, 2, tzinfo=timezone.utc),
            event_name=EventType.INCIDENT,
            incident_type=IncidentType.UNKNOWN,
            bundle_id="mock_bundle_id",
        ),
        DeviceInfoEventArtifact(
            tenant_id="mock_tenant_id_foo",
            device_id="mock_device_id_bar",
            timestamp=datetime(2023, 1, 2, tzinfo=timezone.utc),
            event_name=EventType.DEVICE_INFO,
            system_report="mock_report",
            software_versions=[{"version": "1.0", "software_name": "app"}],
            device_type="mock_type",
            last_shutdown=Shutdown(reason=ShutdownReason.UNKNOWN, reason_description=None,
                                   timestamp=datetime(2023, 1, 1, tzinfo=timezone.utc)),
        ),
        CameraServiceEventArtifact(
            tenant_id="mock_tenant_id_foo",
            device_id="mock_device_id_bar",
            timestamp=datetime(2023, 1, 2, tzinfo=timezone.utc),
            event_name=EventType.CAMERA_SERVICE,
            artifact_name="camera_service",
            service_state=GeneralServiceState.UNKNOWN,
            camera_name="mock_camera_name",
            camera_state=[CameraServiceState.UNKNOWN, CameraServiceState.CAMERA_READY]
        )
    ])
    async def test_create_event(self, event_engine: MagicMock, mongo_controller: MongoService,
                                message: Union[CameraServiceEventArtifact, DeviceInfoEventArtifact,
                                               IncidentEventArtifact]):
        """Test for create_event method

        Args:
            event_engine (MagicMock): event engine mock
            mongo_controller (MongoController): class where the tested method is defined
            message (Union[CameraServiceEventArtifact, DeviceInfoEventArtifact,
                           IncidentEventArtifact]): message artifact for the event
        """

        event_engine.save = AsyncMock()
        await mongo_controller.create_event(message)
        event_engine.save.assert_called_once()

    async def test_update_snapshots_correlations(self, correlated_snapshots: list[str], video_id: str,
                                                 snapshot_engine: MagicMock, mongo_controller: MongoService):
        """Test for update_snapshots_correlations method

        Args:
            correlated_snapshots (list[str]): list of correlated snapshot ids
            video_id (str): correlated video id
            snapshot_engine (MagicMock): snapshot engine mock
            mongo_controller (MongoController): class where the tested method is defined
        """

        snapshot_engine.update_many = AsyncMock()
        update_snapshot = {
            "$addToSet": {
                "recording_overview.source_videos": {'$each': ['video_id_1']}
            }
        }

        filter_correlated = {
            "video_id": {"$in": correlated_snapshots}
        }

        await mongo_controller.update_snapshots_correlations(correlated_snapshots, video_id)
        snapshot_engine.update_many.assert_called_once_with(filter_correlated, update_snapshot)

    async def test_create_s3video(self, video_engine: MagicMock, mongo_controller: MongoService,
                                  video: S3VideoArtifact, mongo_recordings_controller: MongoRecordingsService):
        """Test for create_s3video method

        Args:
            video_engine (MagicMock): video engine mock
            mongo_controller (MongoController): class where the tested method is defined
            video (S3VideoArtifact): video artifact to be created
        """
        correlated_ids = ["mock_id"]
        video_engine.update_one = AsyncMock()
        mongo_recordings_controller.update_videos_correlations = AsyncMock()

        await mongo_controller.upsert_video(video, correlated_ids)
        video_engine.update_one.assert_called_once_with(query={
            "video_id": video.artifact_id,
            "_media_type": "video"
        },
            command=ANY,
            upsert=True
        )
        mongo_recordings_controller.update_videos_correlations.assert_called_once_with(
            [video.artifact_id], correlated_ids)

    async def test_load_device_video_signals_data(self, mongo_controller: MongoService, mongo_signals_controller: MongoSignalsService, mongo_recordings_controller: MongoRecordingsService):
        """Test for load_device_video_signals_data method

        Args:
            MongoService (MongoService): mongo controller
            mongo_signals_controller (MongoSignalsService): mongo signals controller
            mongo_recordings_controller (MongoRecordingsService): mongo recordings controller
        """
        # GIVEN
        video_signals_artifact = Mock()
        video_signals_artifact.data = Mock()
        video_signals_artifact.aggregated_metadata = Mock()
        video_signals_artifact.correlation_id = Mock()

        mongo_signals_controller.save_signals = AsyncMock()
        mongo_recordings_controller.upsert_video_aggregated_metadata = AsyncMock()

        # WHEN
        await mongo_controller.load_device_video_signals_data(video_signals_artifact)

        # THEN
        mongo_signals_controller.save_signals.assert_called_once_with(
            video_signals_artifact.data, "MDFParser", video_signals_artifact.correlation_id)
        mongo_recordings_controller.upsert_video_aggregated_metadata.assert_called_once_with(
            video_signals_artifact.aggregated_metadata, video_signals_artifact.correlation_id)
