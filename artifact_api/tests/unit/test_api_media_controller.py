"Unit tests for media_controller"
from unittest.mock import Mock, AsyncMock
from datetime import datetime, timezone, timedelta
import pytest
from base.model.artifacts import S3VideoArtifact, SnapshotArtifact, RecorderType, TimeWindow
from artifact_api.api.media_controller import MediaController


class TestMediaController:  # pylint: disable=duplicate-code
    "Unit tests for media_controller endpoints"

    @pytest.fixture(name="media_controller")
    def fixture_generate_voxel_service(self) -> MediaController:
        """generate MediaController
        """
        return MediaController()

    @pytest.fixture(name="mock_mongo_service")
    def fixture_mongo_service(self) -> AsyncMock:
        """mocks mongo_service function
        """
        var = AsyncMock()
        var.get_correlated_snapshots_for_video = AsyncMock()
        var.create_video = AsyncMock()
        var.update_snapshots_correlations = AsyncMock()
        return var

    @pytest.fixture(name="mock_voxel_service")
    def fixture_voxel_service(self) -> Mock:
        """mocks voxel_service function
        """
        var = Mock()
        var.update_voxel_video_correlated_snapshots = Mock()
        var.create_voxel_video = Mock()
        return var

    @pytest.fixture()
    def video_artifact(self) -> S3VideoArtifact:
        """VideoArtifact for testing."""
        return S3VideoArtifact(
            artifact_id="datanauts/test123",
            rcc_s3_path="s3://dev-rcd-anonymized-video-files/datanauts/test123.mp4",
            s3_path="s3://dev-rcd-anonymized-video-files/datanauts/test123.mp4",
            tenant_id="datanauts",
            device_id="DATANAUTS_DEV_01",
            recorder=RecorderType.INTERIOR,
            timestamp=datetime.now(tz=timezone.utc),
            end_timestamp=datetime.now(tz=timezone.utc),
            upload_timing=TimeWindow(start=(datetime.now() - timedelta(minutes=50)), end=datetime.now()),
            uuid="uuid",
            footage_id="footage_id",
            recordings=[],
            correlated_artifacts=["a", "b"]
        )

    @pytest.fixture()
    def snapshot_artifact(self) -> SnapshotArtifact:
        """VideoArtifact for testing."""
        return SnapshotArtifact(
            artifact_id="datanauts/test123",
            rcc_s3_path="s3://dev-rcd-anonymized-video-files/datanauts/test123.jpeg",
            s3_path="s3://dev-rcd-anonymized-video-files/datanauts/test123.jpeg",
            tenant_id="datanauts",
            device_id="DATANAUTS_DEV_01",
            recorder=RecorderType.SNAPSHOT,
            timestamp=datetime.now(tz=timezone.utc),
            end_timestamp=datetime.now(tz=timezone.utc),
            upload_timing=TimeWindow(start=(datetime.now() - timedelta(minutes=50)), end=datetime.now()),
            uuid="uuid",
            footage_id="footage_id",
            recordings=[],
            correlated_artifacts=["a", "b"]
        )

    @pytest.mark.unit
    async def test_process_video_artifact(self, video_artifact: S3VideoArtifact,
                                          mock_mongo_service: AsyncMock,
                                          mock_voxel_service: Mock,
                                          media_controller: MediaController):
        """
        Test a video process

        """

        # GIVEN

        # WHEN
        await media_controller.process_video_artifact(video_artifact, mock_mongo_service, mock_voxel_service)
        # THEN
        mock_mongo_service.get_correlated_snapshots_for_video.assert_called_once_with(video_artifact)
        mock_mongo_service.create_video.assert_called_once_with(video_artifact)
        mock_mongo_service.update_snapshots_correlations.assert_called_once_with(
            video_artifact.correlated_artifacts, video_artifact.artifact_id)
        mock_voxel_service.update_voxel_video_correlated_snapshots.assert_called_once_with(
            raw_correlated_filepaths=video_artifact.correlated_artifacts,
            raw_filepath=video_artifact.s3_path,
            tenant_id=video_artifact.tenant_id)
        mock_voxel_service.create_voxel_video.assert_called_once_with(artifact=video_artifact)

    @pytest.mark.unit
    async def test_process_snapshot_artifact(self, snapshot_artifact: SnapshotArtifact,
                                             mock_mongo_service: AsyncMock,
                                             mock_voxel_service: Mock,
                                             media_controller: MediaController):
        """
        Test a snapshot process

        """

        # GIVEN

        # WHEN
        await media_controller.process_snapshot_artifact(snapshot_artifact, mock_mongo_service, mock_voxel_service)
        # THEN
        mock_mongo_service.get_correlated_videos_for_snapshot.assert_called_once_with(snapshot_artifact)
        mock_mongo_service.create_snapshot.assert_called_once_with(snapshot_artifact)
        mock_mongo_service.update_videos_correlations.assert_called_once_with(
            snapshot_artifact.correlated_artifacts, snapshot_artifact.artifact_id)
        mock_voxel_service.update_voxel_video_correlated_snapshots.assert_called_once_with(
            raw_correlated_filepaths=snapshot_artifact.correlated_artifacts,
            raw_filepath=snapshot_artifact.s3_path,
            tenant_id=snapshot_artifact.tenant_id)
        mock_voxel_service.create_voxel_snapshot.assert_called_once_with(snapshot_artifact)