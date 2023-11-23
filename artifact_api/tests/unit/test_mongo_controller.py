"""Tests for mongodb controller"""

from datetime import datetime, timezone
from typing import Union
from unittest.mock import AsyncMock, MagicMock, Mock
from pytest import fixture, mark
from base.model.artifacts import (CameraServiceEventArtifact, DeviceInfoEventArtifact, IncidentEventArtifact,
                                  RecorderType, Recording, Resolution, S3VideoArtifact, SnapshotArtifact,
                                  TimeWindow)
from base.model.event_types import (CameraServiceState, EventType, GeneralServiceState, IncidentType,
                                    Shutdown, ShutdownReason)
from artifact_api.mongo_controller import MongoController


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
            tenant_id="foo",
            device_id="bar",
            s3_path="s3://dev-bucket/key",
            recorder=RecorderType.SNAPSHOT,
            timestamp=datetime.fromisoformat("2023-04-13T07:14:15.770982+00:00"),
            end_timestamp=datetime.fromisoformat("2023-04-13T07:14:15.770982+00:00"),
            upload_timing=TimeWindow(
                start="2023-04-13T08:00:00+00:00",  # type: ignore
                end="2023-04-13T08:01:00+00:00"),  # type: ignore
            uuid="abc",
            correlated_artifacts=["correlated_1", "correlated_2"]
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
            location={},
            incident_type=IncidentType.UNKNOWN,
            bundle_id="mock_bundle_id",
        )

    async def test_create_snapshot(self, snapshot_engine: MagicMock, mongo_controller: MongoController,
                                   snapshot_artifact: SnapshotArtifact):
        """Test for create_snapshot method

        Args:
            snapshot_engine (MagicMock): snapshot engine mock
            mongo_controller (MongoController): class where the tested method is defined
            snapshot_artifact (SnapshotArtifact): snapshot artifact to be ingested
        """

        snapshot_engine.save = AsyncMock()
        await mongo_controller.create_snapshot(snapshot_artifact)
        snapshot_engine.save.assert_called_once()

    async def test_update_videos_correlations(self, correlated_videos: list[str], snapshot_id: str,
                                              video_engine: MagicMock, mongo_controller: MongoController):
        """Test for update_videos_correlations method

        Args:
            correlated_videos (list[str]): list of correlated video ids
            snapshot_id (str): id of snapshot
            video_engine (MagicMock): video engine mock
            mongo_controller (MongoController): class where the tested method is defined
        """

        video_engine.update_many = AsyncMock()
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

        await mongo_controller.update_videos_correlations(correlated_videos, snapshot_id)
        video_engine.update_many.assert_called_once_with(filter_correlated, update_video)

    async def test_get_correlated_videos_for_snapshot(self, video_engine: MagicMock,
                                                      mongo_controller: MongoController):
        """Test for get_correlated_videos_for_snapshot method

        Args:
            video_engine (MagicMock): video engine mock
            mongo_controller (MongoController): class where the tested method is defined
        """

        message = Mock(device_id="mock_device_id_bar", timestamp=111112, end_timestamp=111113)

        correlated = {
            "recording_overview.deviceID": message.device_id,
            "recording_overview.recording_time": {"$lte": message.timestamp},
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

        correlated_artifacts = MagicMock()
        correlated_artifacts.__aiter__.return_value = [Mock(filepath=1), Mock(filepath=2)]

        video_engine.find = MagicMock()
        video_engine.find.return_value = correlated_artifacts

        result = await mongo_controller.get_correlated_videos_for_snapshot(message)
        video_engine.find.assert_called_once_with(correlated)

        assert result == [1, 2]

    async def test_get_correlated_snapshots_for_video(self, snapshot_engine: MagicMock,
                                                      mongo_controller: MongoController):
        """Test for get_correlated_snapshots_for_video method

        Args:
            snapshot_engine (MagicMock): snapshot engine mock
            mongo_controller (MongoController): class where the tested method is defined
        """

        message = Mock(device_id="mock_device_id_bar", timestamp=111112, end_timestamp=111113)

        correlated = {
            "deviceID": message.device_id,
            "recording_overview.recording_time": {
                "$gte": message.timestamp,
                "$lte": message.end_timestamp
            }
        }

        correlated_artifacts = MagicMock()
        correlated_artifacts.__aiter__.return_value = [Mock(filepath=1), Mock(filepath=2)]

        snapshot_engine.find = MagicMock()
        snapshot_engine.find.return_value = correlated_artifacts

        result = await mongo_controller.get_correlated_snapshots_for_video(message)
        snapshot_engine.find.assert_called_once_with(correlated)

        assert result == [1, 2]

    @mark.parametrize("message", [
        IncidentEventArtifact(
            tenant_id="mock_tenant_id_foo",
            device_id="mock_device_id_bar",
            timestamp=datetime(2023, 1, 2, tzinfo=timezone.utc),
            event_name=EventType.INCIDENT,
            location={},
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
    async def test_create_event(self, event_engine: MagicMock, mongo_controller: MongoController,
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
                                                 snapshot_engine: MagicMock, mongo_controller: MongoController):
        """Test for update_snapshots_correlations method

        Args:
            correlated_snapshots (list[str]): list of correlated snapshot ids
            video_id (str): correlated video id
            snapshot_engine (MagicMock): snapshot engine mock
            mongo_controller (MongoController): class where the tested method is defined
        """

        snapshot_engine.update_many = AsyncMock()
        update_snapshot = {
            "$push": {
                "recording_overview.source_videos": video_id
            }
        }

        filter_correlated = {
            "video_id": {"$in": correlated_snapshots}
        }

        await mongo_controller.update_snapshots_correlations(correlated_snapshots, video_id)
        snapshot_engine.update_many.assert_called_once_with(filter_correlated, update_snapshot)

    async def test_create_s3video(self, video_engine: MagicMock, mongo_controller: MongoController,
                                  video: S3VideoArtifact):
        """Test for create_s3video method

        Args:
            video_engine (MagicMock): video engine mock
            mongo_controller (MongoController): class where the tested method is defined
            video (S3VideoArtifact): video artifact to be created
        """

        video_engine.save = AsyncMock()
        await mongo_controller.create_video(video)
        video_engine.save.assert_called_once()
