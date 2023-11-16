"""Tests for mongodb models"""

from datetime import datetime, timezone, timedelta
from pytest import fixture, mark, raises
from pydantic import ValidationError

from base.model.event_types import (CameraServiceState, EventType,
                                    GeneralServiceState, IncidentType,
                                    Shutdown, ShutdownReason)
from artifact_api.models.mongo_models import (DBSnapshotRecordingOverview, DBSnapshotArtifact,
                                              DBCameraServiceEventArtifact, DBDeviceInfoEventArtifact,
                                              DBIncidentEventArtifact, DBS3VideoArtifact,
                                              DBVideoRecordingOverview)


@mark.unit
class TestMongoModels:  # pylint: disable=no-member, duplicate-code
    """Test class for mongodb models"""

    @fixture
    def db_snapshot_recording_overview(self) -> DBSnapshotRecordingOverview:
        """Fixture for DB Snapshot Overview artifact

        Returns:
            DBSnapshotRecordingOverview: DB Snapshot Overview artifact
        """
        return DBSnapshotRecordingOverview(
            devcloud_id="mock_devcloud_id",
            device_id="mock_device_id",
            tenant_id="mock_tenant_id",
            recording_time="2023-10-26T12:00:00",
            source_videos=["mock_1.mp4", "mock_2.mp4"]
        )

    @fixture
    def db_video_recording_overview(self) -> DBVideoRecordingOverview:
        """Fixture for DB Video Recording Overview artifact

        Returns:
            DBVideoRecordingOverview: DB Video Recording Overview artifact
        """

        snapshots = 1
        devcloud_id = "mock_dev_id"
        device_id = "mock_device_id"
        actual_duration = 100
        timestamp = datetime(2023, 1, 1, tzinfo=timezone.utc)
        correlated_artifacts = ["correlated_artifact1", "correlated_artifact2"]
        tenant_id = "mock_tenant_id"
        time_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")

        return DBVideoRecordingOverview(
            snapshots=snapshots,
            devcloud_id=devcloud_id,
            device_id=device_id,
            length=str(timedelta(seconds=actual_duration)),
            recording_time=timestamp,
            recording_duration=actual_duration,
            snapshots_paths=correlated_artifacts,
            tenant_id=tenant_id,
            time=time_str,
            chc_duration=0,
            gnss_coverage=0,
            max_audio_loudness=0,
            max_person_count=0,
            mean_audio_bias=0,
            median_person_count=0,
            number_chc_events=0,
            ride_detection_people_count_after=0,
            ride_detection_people_count_before=0,
            sum_door_closed=0,
            variance_person_count=0
        )

    @mark.unit
    def test_db_snapshot_recording_overview(self, db_snapshot_recording_overview: DBSnapshotRecordingOverview):
        """Test for DB Snapshot Recording Overview mongodb model

        Args:
            db_snapshot_recording_overview (DBSnapshotRecordingOverview): DB Snapshot Recording Overview mongodb model
        """

        invalid_data = {
            "devcloud_id": "mock_devcloud_id",  # Should use "devcloudid" alias
            "device_id": "mock_device_id",  # Should use "deviceID" alias
            "tenant_id": "mock_tenant_id",  # Should use "tenantID" alias
            "recording_time": "2023/10/26",  # Invalid datetime format
            "source_videos": "mock_2.mp4",  # Should be a list
        }

        assert db_snapshot_recording_overview.devcloud_id == "mock_devcloud_id"
        assert db_snapshot_recording_overview.device_id == "mock_device_id"
        assert db_snapshot_recording_overview.tenant_id == "mock_tenant_id"
        assert db_snapshot_recording_overview.recording_time == datetime(2023, 10, 26, 12, 0, 0)
        assert db_snapshot_recording_overview.source_videos == ["mock_1.mp4", "mock_2.mp4"]

        with raises(ValidationError):
            DBSnapshotRecordingOverview(**invalid_data)

    @mark.unit
    def test_dbsnapshot_artifact(self, db_snapshot_recording_overview: DBSnapshotRecordingOverview):
        """Test for DB Snapshot Artifact mongodb model

        Args:
            db_snapshot_recording_overview (DBSnapshotRecordingOverview): DB Snapshot Artifact mongodb model
        """

        valid_db_snapshot_artifact = DBSnapshotArtifact(
            video_id="mock_video_id",
            filepath="s3://bucket/key",
            recording_overview=db_snapshot_recording_overview
        )

        assert valid_db_snapshot_artifact.video_id == "mock_video_id"
        assert valid_db_snapshot_artifact.media_type == "image"
        assert valid_db_snapshot_artifact.filepath == "s3://bucket/key"
        assert isinstance(valid_db_snapshot_artifact.recording_overview, DBSnapshotRecordingOverview)

        invalid_data = {
            "video_id": "mock_video_id",
            "_media_type": "video",
            "filepath": "s3://bucket/key",
            "recording_overview": {}  # Invalid format
        }

        with raises(ValidationError):
            DBSnapshotArtifact(**invalid_data)

    @mark.unit
    def test_dbcamera_service_event_artifact(self):
        """Test for DB Camera Service Event Artifact mongodb model
        """

        event = DBCameraServiceEventArtifact(
            tenant_id="mock_tenant_id",
            device_id="mock_device_id",
            s3_path="s3://bucket/key",
            timestamp=datetime(2023, 1, 1, tzinfo=timezone.utc),
            event_name=EventType.CAMERA_SERVICE,
            artifact_name="camera_service",
            service_state=GeneralServiceState.UNKNOWN,
            camera_name="mock_camera",
            camera_state=[CameraServiceState.UNKNOWN, CameraServiceState.CAMERA_READY]
        )

        assert event.tenant_id == "mock_tenant_id"
        assert event.device_id == "mock_device_id"
        assert event.s3_path == "s3://bucket/key"
        assert event.timestamp == datetime(2023, 1, 1, tzinfo=timezone.utc)
        assert event.event_name == EventType.CAMERA_SERVICE
        assert event.artifact_name == "camera_service"
        assert event.service_state == GeneralServiceState.UNKNOWN
        assert event.camera_name == "mock_camera"
        assert event.camera_state == [CameraServiceState.UNKNOWN, CameraServiceState.CAMERA_READY]

        with raises(ValidationError):
            DBCameraServiceEventArtifact(
                tenant_id="mock_tenant_id",
                device_id="mock_device_id",
                s3_path="bad_path",
                timestamp=datetime(2023, 1, 1),
                event_name="bad_name",
                artifact_name="bad_artifact",
                camera_name="mock_camera",
                camera_state=[CameraServiceState.UNKNOWN, CameraServiceState.CAMERA_READY]
            )

    @mark.unit
    def test_dbdevice_info_event_artifact(self):
        """Test for DB Device Info Event Artifact mongodb model
        """

        event = DBDeviceInfoEventArtifact(
            tenant_id="mock_tenant_id",
            device_id="mock_device_id",
            s3_path="s3://bucket/key",
            timestamp=datetime(2023, 1, 1, tzinfo=timezone.utc),
            event_name=EventType.DEVICE_INFO,
            system_report="mock_report",
            software_versions=[{"version": "1.0", "software_name": "app"}],
            device_type="mock_type",
            last_shutdown=Shutdown(reason=ShutdownReason.UNKNOWN,
                                   reason_description=None,
                                   timestamp=datetime(2023, 1, 1, tzinfo=timezone.utc)),
        )

        assert event.tenant_id == "mock_tenant_id"
        assert event.device_id == "mock_device_id"
        assert event.s3_path == "s3://bucket/key"
        assert event.timestamp == datetime(2023, 1, 1, tzinfo=timezone.utc)
        assert event.event_name == EventType.DEVICE_INFO
        assert event.system_report == "mock_report"
        assert event.software_versions == [{"version": "1.0", "software_name": "app"}]
        assert event.device_type == "mock_type"
        assert event.last_shutdown.reason == ShutdownReason.UNKNOWN
        assert event.last_shutdown.timestamp == datetime(2023, 1, 1, tzinfo=timezone.utc)

        with raises(ValidationError):
            DBDeviceInfoEventArtifact(
                tenant_id="mock_tenant_id",
                device_id="mock_device_id",
                s3_path="bad_path",
                timestamp=datetime(2023, 1, 1),
                event_name=EventType.DEVICE_INFO,
                system_report="mock_report",
                software_versions=[["bad_version"]],
                device_type="mock_type",
                last_shutdown=Shutdown(reason=ShutdownReason.UNKNOWN,
                                       timestamp=datetime(2023, 1, 1, tzinfo=timezone.utc)),
            )

    @mark.unit
    def test_dbincident_event_artifact(self):
        """Test for DB Incident Event Artifact mongodb model
        """
        event = DBIncidentEventArtifact(
            tenant_id="mock_tenant_id",
            device_id="mock_device_id",
            s3_path="s3://bucket/key",
            timestamp=datetime(2023, 1, 1, tzinfo=timezone.utc),
            event_name=EventType.INCIDENT,
            location={},
            incident_type=IncidentType.UNKNOWN,
            bundle_id="mock_bundle_id",
        )

        assert event.tenant_id == "mock_tenant_id"
        assert event.device_id == "mock_device_id"
        assert event.s3_path == "s3://bucket/key"
        assert event.timestamp == datetime(2023, 1, 1, tzinfo=timezone.utc)
        assert event.event_name == EventType.INCIDENT
        assert event.incident_type == IncidentType.UNKNOWN
        assert event.bundle_id == "mock_bundle_id"

        with raises(ValidationError):
            DBIncidentEventArtifact(
                tenant_id="mock_tenant_id",
                device_id="mock_device_id",
                s3_path="bad_path",
                timestamp=datetime(2023, 1, 1),
                event_name="bad_name",
                location={},
                incident_type="bad_type",
                bundle_id="mock_bundle_id",
            )

    @mark.unit
    def test_db_video_recording_overview(self, db_video_recording_overview: DBVideoRecordingOverview):
        """Test for DB Video Recording Overview mongodb model

        Args:
            db_video_recording_overview (DBVideoRecordingOverview): DB Video Recording Overview mongodb model
        """

        assert db_video_recording_overview.snapshots == 1
        assert db_video_recording_overview.devcloud_id == "mock_dev_id"
        assert db_video_recording_overview.device_id == "mock_device_id"
        assert db_video_recording_overview.length == "0:01:40"
        assert db_video_recording_overview.recording_time == datetime(2023, 1, 1, tzinfo=timezone.utc)
        assert db_video_recording_overview.recording_duration == 100
        assert db_video_recording_overview.snapshots_paths == ["correlated_artifact1", "correlated_artifact2"]
        assert db_video_recording_overview.tenant_id == "mock_tenant_id"
        assert db_video_recording_overview.time == "2023-01-01 00:00:00"
        assert db_video_recording_overview.chc_duration == 0
        assert db_video_recording_overview.gnss_coverage == 0
        assert db_video_recording_overview.max_audio_loudness == 0
        assert db_video_recording_overview.max_person_count == 0
        assert db_video_recording_overview.mean_audio_bias == 0
        assert db_video_recording_overview.median_person_count == 0
        assert db_video_recording_overview.number_chc_events == 0
        assert db_video_recording_overview.ride_detection_people_count_after == 0
        assert db_video_recording_overview.ride_detection_people_count_before == 0
        assert db_video_recording_overview.sum_door_closed == 0
        assert db_video_recording_overview.variance_person_count == 0

    @mark.unit
    def test_db_s3video_artifact(self, db_video_recording_overview: DBVideoRecordingOverview):
        """Test for DB Video Recording Overview mongodb model

        Args:
            db_video_recording_overview (DBVideoRecordingOverview): DB Video Recording Overview mongodb model
        """

        video_id = "mock_video_id"
        filepath = "s3://bucket/key"
        resolution = "1920x1080"
        recording_overview = db_video_recording_overview

        video_artifact = DBS3VideoArtifact(
            video_id=video_id,
            filepath=filepath,
            resolution=resolution,
            recording_overview=db_video_recording_overview
        )

        assert video_artifact.video_id == "mock_video_id"
        assert video_artifact.mdf_available == "No"
        assert video_artifact.media_type == "video"
        assert video_artifact.filepath == "s3://bucket/key"
        assert video_artifact.resolution == "1920x1080"
        assert video_artifact.recording_overview == recording_overview
