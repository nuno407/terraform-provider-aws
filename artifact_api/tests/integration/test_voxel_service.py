""" Integration tests for voxel service. """
import os
from datetime import datetime, timezone, timedelta
from unittest.mock import patch
import pytest
from pydantic import SkipValidation
from kink import di

import fiftyone as fo
from fiftyone import ViewField

from base.model.artifacts import S3VideoArtifact, SnapshotArtifact, RecorderType, TimeWindow
from base.model.config.dataset_config import DatasetConfig
from base.model.config.policy_config import PolicyConfig
from base.model.base_model import S3Path
from artifact_api.voxel import VoxelService, VoxelConfig


def mock_anon(value: str) -> str:
    """mocks get_anonymized_path_from_raw function
    """
    video_repl = value.replace(".mp4", "_anonymized.mp4")
    snap_repl = video_repl.replace(".png", "_anonymized.png")
    return snap_repl


class TestVoxelService:
    """ Integration tests for voxel service. """

    def video_artifact(self, s3_path: str) -> S3VideoArtifact:
        """VideoArtifact for testing."""
        class _S3VideoArtifactWithS3Validation(S3VideoArtifact):
            s3_path: SkipValidation[S3Path]  # Overriding validation of file because we use local paths

        return _S3VideoArtifactWithS3Validation(
            rcc_s3_path="s3://not-needed/not-needed/",
            s3_path=s3_path,
            artifact_id="",
            raw_s3_path="s3://raw/foo/bar.something",
            anonymized_s3_path="s3://anonymized/foo/bar.something",
            tenant_id="datanauts",
            device_id="DATANAUTS_DEV_01",
            recorder=RecorderType.INTERIOR,
            timestamp=datetime.now(tz=timezone.utc),
            end_timestamp=datetime.now(tz=timezone.utc),
            upload_timing=TimeWindow(start=(datetime.now() - timedelta(minutes=5)), end=datetime.now()),
            uuid="uuid",
            footage_id="footage_id",
            recordings=[],
        )

    def snapshot_artifact(self, s3_path: str) -> SnapshotArtifact:
        """SnapshotArtifact for testing."""
        class _SnapshotArtifactWithS3Validation(SnapshotArtifact):
            s3_path: SkipValidation[S3Path]  # Overriding validation of file because we use local paths

        return _SnapshotArtifactWithS3Validation(
            s3_path=s3_path,
            artifact_id="",
            raw_s3_path="s3://raw/foo/bar.something",
            anonymized_s3_path="s3://anonymized/foo/bar.something",
            tenant_id="datanauts",
            device_id="DATANAUTS_DEV_02",
            recorder=RecorderType.SNAPSHOT,
            timestamp=datetime.now(tz=timezone.utc),
            end_timestamp=datetime.now(tz=timezone.utc),
            upload_timing=TimeWindow(start=(datetime.now() - timedelta(minutes=5)), end=datetime.now()),
            uuid="uuid")

    @pytest.fixture()
    def test_images_paths(self, request: pytest.FixtureRequest) -> list[str]:
        """ List of available images paths. """
        test_dir = os.path.dirname(request.module.__file__)
        return [os.path.join(test_dir, "test_data", "images", name) for name in ["a.png", "b.png", "c.png", "d.png"]]

    @pytest.fixture()
    def test_videos_paths(self, request: pytest.FixtureRequest) -> list[str]:
        """ List of available videos paths. """
        test_dir = os.path.dirname(request.module.__file__)
        return [os.path.join(test_dir, "test_data", "videos", name) for name in ["a.mp4", "b.mp4", "c.mp4", "d.mp4"]]

    @pytest.fixture()
    @patch("artifact_api.voxel.voxel_base_models.get_anonymized_path_from_raw", new=mock_anon)
    def voxel_service(self, test_videos_paths: list[str], test_images_paths: list[str]) -> VoxelService:
        """Generate VoxelService"""
        dataset_config = DatasetConfig(default_dataset="datanauts", tag="RC")
        policy_config = PolicyConfig(default_policy_document="test_doc")
        voxel_config = VoxelConfig(dataset_mapping=dataset_config, policy_mapping=policy_config)
        di[PolicyConfig] = policy_config
        voxel_service = VoxelService(voxel_config=voxel_config)
        for name in test_videos_paths:
            voxel_service.create_voxel_video(self.video_artifact(name))
        for name in test_images_paths:
            voxel_service.create_voxel_snapshot(self.snapshot_artifact(name))
        return voxel_service

    @pytest.mark.integration
    @patch("artifact_api.voxel.voxel_video.get_anonymized_path_from_raw", new=mock_anon)
    @patch("artifact_api.voxel.voxel_snapshot.get_anonymized_path_from_raw", new=mock_anon)
    def update_voxel_snapshots_with_correlated_video(
            self,
            voxel_service: VoxelService,
            test_videos_paths: list[str],
            test_images_paths: list[str],
    ):
        """Test update_voxel_snapshots_with_correlated_video"""
        # Video at position 0 of the array is related with the snapshots 2 and 3.
        # Also video at position 3 is related with image in position 3
        # In this test there is no relation between samples still
        # GIVEN
        dataset = fo.load_dataset("datanauts_snapshots")
        # WHEN
        voxel_service.update_voxel_snapshots_with_correlated_video(
            raw_filepath=test_videos_paths[0], raw_correlated_filepaths=[
                test_images_paths[2], test_images_paths[3]], tenant_id="datanauts")
        voxel_service.update_voxel_snapshots_with_correlated_video(raw_filepath=test_videos_paths[3],
                                                                   raw_correlated_filepaths=[test_images_paths[3]],
                                                                   tenant_id="datanauts")

        # THEN
        assert len(dataset) == 4
        assert dataset.one(ViewField("raw_filepath").starts_with(test_images_paths[0]))["source_videos"] is None
        assert dataset.one(ViewField("raw_filepath").starts_with(test_images_paths[1]))["source_videos"] is None
        assert dataset.one(ViewField("raw_filepath").starts_with(test_images_paths[2]))[
            "source_videos"] == [mock_anon(test_videos_paths[0])]
        assert dataset.one(ViewField("raw_filepath").starts_with(test_images_paths[3]))[
            "source_videos"] == [mock_anon(test_videos_paths[0]), mock_anon(test_videos_paths[3])]

    @pytest.mark.integration
    @patch("artifact_api.voxel.voxel_snapshot.get_anonymized_path_from_raw", new=mock_anon)
    @patch("artifact_api.voxel.voxel_video.get_anonymized_path_from_raw", new=mock_anon)
    def test_update_voxel_video_with_correlated_snapshots(
            self,
            voxel_service: VoxelService,
            test_videos_paths: list[str],
            test_images_paths: list[str]):
        """Test update_voxel_video_correlated_snapshot"""
        # Snapshot at position 3 of the array is related with the video on position 0.
        # In this test there is no relation between samples still
        # GIVEN
        dataset = fo.load_dataset("datanauts")
        # WHEN
        voxel_service.update_voxel_videos_with_correlated_snapshot(raw_filepath=test_images_paths[3],
                                                                   raw_correlated_filepaths=[test_videos_paths[0]],
                                                                   tenant_id="datanauts")

        # THEN
        assert len(dataset) == 4
        assert dataset.one(ViewField("raw_filepath").starts_with(test_videos_paths[0]))[
            "snapshots_paths"] == [mock_anon(test_images_paths[3])]
        assert dataset.one(ViewField("raw_filepath").starts_with(test_videos_paths[1]))["snapshots_paths"] is None
        assert dataset.one(ViewField("raw_filepath").starts_with(test_videos_paths[2]))["snapshots_paths"] is None
        assert dataset.one(ViewField("raw_filepath").starts_with(test_videos_paths[3]))["snapshots_paths"] is None
