""" Integration tests for voxel service. """
import os
from datetime import datetime
from unittest.mock import patch
import pytest
from pydantic import SkipValidation
from kink import di

import fiftyone as fo
from fiftyone import ViewField

from base.model.artifacts import (S3VideoArtifact, SnapshotArtifact, RecorderType,
                                  TimeWindow, VideoUploadRule, SnapshotUploadRule,
                                  SelectorRule, RuleOrigin)
from base.model.config.dataset_config import DatasetConfig
from base.model.config.policy_config import PolicyConfig
from artifact_api.voxel import VoxelService, VoxelConfig


def mock_anon(filepath: str) -> str:
    """mocks get_anonymized_path_from_raw function
    """
    video_repl = filepath.replace(".mp4", "_anonymized.mp4")
    snap_repl = video_repl.replace(".png", "_anonymized.png")
    return snap_repl


class VideoUploadRuleWithoutS3Validation(VideoUploadRule):
    """VideoUploadRule for testing."""
    raw_file_path: SkipValidation[str]
    footage_from: SkipValidation[datetime]
    footage_to: SkipValidation[datetime]


class SnapshotUploadRuleWithoutS3Validation(SnapshotUploadRule):
    """SnapshotUploadRule for testing."""
    raw_file_path: SkipValidation[str]
    snapshot_timestamp: SkipValidation[datetime]


class TestVoxelService:
    """ Integration tests for voxel service. """

    def video_artifact(self, s3_path: str) -> S3VideoArtifact:
        """VideoArtifact for testing."""
        class _S3VideoArtifactWithoutS3Validation(S3VideoArtifact):
            # Overriding validation of file because we use local paths
            s3_path: SkipValidation[str]
            raw_s3_path: SkipValidation[str]
            anonymized_s3_path: SkipValidation[str]
            end_timestamp: SkipValidation[datetime]
            timestamp: SkipValidation[datetime]

        return _S3VideoArtifactWithoutS3Validation(
            rcc_s3_path="s3://not-needed/not-needed/",
            s3_path=s3_path,
            artifact_id="",
            raw_s3_path=s3_path,
            anonymized_s3_path=s3_path,
            tenant_id="datanauts",
            device_id="DATANAUTS_DEV_01",
            recorder=RecorderType.INTERIOR,
            timestamp=datetime(
                year=2023,
                month=12,
                day=12,
                hour=9),
            end_timestamp=datetime(
                year=2023,
                month=12,
                day=12,
                hour=10),
            upload_timing=TimeWindow(
                start=datetime(
                    year=2023,
                    month=12,
                    day=12,
                    hour=7),
                end=datetime(
                    year=2023,
                    month=12,
                    day=12,
                    hour=8)),
            uuid="uuid",
            footage_id="footage_id",
            recordings=[],
        )

    def snapshot_artifact(self, s3_path: str) -> SnapshotArtifact:
        """SnapshotArtifact for testing."""
        class _SnapshotArtifactWithoutS3Validation(SnapshotArtifact):
            # Overriding validation of file because we use local paths
            s3_path: SkipValidation[str]
            raw_s3_path: SkipValidation[str]
            anonymized_s3_path: SkipValidation[str]
            end_timestamp: SkipValidation[datetime]
            timestamp: SkipValidation[datetime]

        return _SnapshotArtifactWithoutS3Validation(
            s3_path=s3_path,
            artifact_id="",
            raw_s3_path=s3_path,
            anonymized_s3_path=s3_path,
            tenant_id="datanauts",
            device_id="DATANAUTS_DEV_02",
            recorder=RecorderType.SNAPSHOT,
            timestamp=datetime(year=2023, month=12, day=12, hour=9),
            end_timestamp=datetime(year=2023, month=12, day=12, hour=10),
            upload_timing=TimeWindow(start=datetime(2023, 12, 12, hour=7), end=datetime(2023, 12, 12, hour=8)),
            uuid="uuid")

    @pytest.fixture()
    def test_images_artifacts(self, request: pytest.FixtureRequest) -> list[SnapshotArtifact]:
        """ List of available images paths. """
        test_dir = os.path.dirname(request.module.__file__)
        return [self.snapshot_artifact(os.path.join(test_dir, "test_data", "images", name))
                for name in ["a.png", "b.png", "c.png", "d.png"]]

    @pytest.fixture()
    def test_videos_artifacts(self, request: pytest.FixtureRequest) -> list[S3VideoArtifact]:
        """ List of available videos paths. """
        test_dir = os.path.dirname(request.module.__file__)
        return [self.video_artifact(os.path.join(test_dir, "test_data", "videos", name))
                for name in ["a.mp4", "b.mp4", "c.mp4", "d.mp4"]]

    @pytest.fixture()
    @patch("artifact_api.voxel.voxel_video.get_anonymized_path_from_raw", new=mock_anon)
    @patch("artifact_api.voxel.voxel_snapshot.get_anonymized_path_from_raw", new=mock_anon)
    def voxel_service(self, test_videos_artifacts: list[str], test_images_artifacts: list[str]) -> VoxelService:
        """Generate VoxelService"""
        dataset_config = DatasetConfig(default_dataset="datanauts", tag="RC")
        policy_config = PolicyConfig(default_policy_document="test_doc")
        voxel_config = VoxelConfig(dataset_mapping=dataset_config, policy_mapping=policy_config)
        di[PolicyConfig] = policy_config
        voxel_service = VoxelService(voxel_config=voxel_config)
        fo.delete_datasets("*")
        for artifact in test_videos_artifacts:
            voxel_service.create_voxel_video(artifact)
        for artifact in test_images_artifacts:
            voxel_service.create_voxel_snapshot(artifact)
        return voxel_service

    @pytest.mark.integration
    @patch("artifact_api.voxel.voxel_video.get_anonymized_path_from_raw", new=mock_anon)
    @patch("artifact_api.voxel.voxel_snapshot.get_anonymized_path_from_raw", new=mock_anon)
    def update_voxel_snapshots_with_correlated_video(
            self,
            voxel_service: VoxelService,
            test_videos_artifacts: list[S3VideoArtifact],
            test_images_artifacts: list[SnapshotArtifact],
    ):
        """Test update_voxel_snapshots_with_correlated_video"""
        # Video at position 0 of the array is related with the snapshots 2 and 3.
        # Also video at position 3 is related with image in position 3
        # In this test there is no relation between samples still
        # GIVEN
        dataset = fo.load_dataset("datanauts_snapshots")
        # WHEN
        voxel_service.update_voxel_snapshots_with_correlated_video(
            raw_filepath=test_videos_artifacts[0].raw_s3_path, raw_correlated_filepaths=[
                test_images_artifacts[2].raw_s3_path, test_images_artifacts[3].raw_s3_path], tenant_id="datanauts")
        voxel_service.update_voxel_snapshots_with_correlated_video(
            raw_filepath=test_videos_artifacts[3].raw_s3_path, raw_correlated_filepaths=[
                test_images_artifacts[3].raw_s3_path], tenant_id="datanauts")

        # THEN
        assert len(dataset) == 4
        assert dataset.one(ViewField("raw_filepath").starts_with(
            test_images_artifacts[0].raw_s3_path))["source_videos"] is None
        assert dataset.one(ViewField("raw_filepath").starts_with(
            test_images_artifacts[1].raw_s3_path))["source_videos"] is None
        assert dataset.one(ViewField("raw_filepath").starts_with(test_images_artifacts[2].raw_s3_path))[
            "source_videos"] == [mock_anon(test_videos_artifacts[0].raw_s3_path)]
        assert dataset.one(ViewField("raw_filepath").starts_with(test_images_artifacts[3]))["source_videos"] == [
            mock_anon(test_videos_artifacts[0].raw_s3_path), mock_anon(test_images_artifacts[3].raw_s3_path)]

    @pytest.mark.integration
    @patch("artifact_api.voxel.voxel_video.get_anonymized_path_from_raw", new=mock_anon)
    @patch("artifact_api.voxel.voxel_snapshot.get_anonymized_path_from_raw", new=mock_anon)
    def test_update_voxel_video_with_correlated_snapshots(
            self,
            voxel_service: VoxelService,
            test_videos_artifacts: list[S3VideoArtifact],
            test_images_artifacts: list[SnapshotArtifact]):
        """Test update_voxel_video_correlated_snapshot"""
        # Snapshot at position 3 of the array is related with the video on position 0.
        # In this test there is no relation between samples still
        # GIVEN
        dataset = fo.load_dataset("datanauts")
        # WHEN
        voxel_service.update_voxel_videos_with_correlated_snapshot(
            raw_filepath=test_images_artifacts[3].raw_s3_path, raw_correlated_filepaths=[
                test_videos_artifacts[0].raw_s3_path], tenant_id="datanauts")

        # THEN
        assert len(dataset) == 4
        assert dataset.one(ViewField("raw_filepath").starts_with(test_videos_artifacts[0].raw_s3_path))[
            "snapshots_paths"] == [mock_anon(test_images_artifacts[3].raw_s3_path)]
        assert dataset.one(ViewField("raw_filepath").starts_with(
            test_videos_artifacts[1].raw_s3_path))["snapshots_paths"] is None
        assert dataset.one(ViewField("raw_filepath").starts_with(
            test_videos_artifacts[2].raw_s3_path))["snapshots_paths"] is None
        assert dataset.one(ViewField("raw_filepath").starts_with(
            test_videos_artifacts[3].raw_s3_path))["snapshots_paths"] is None

    @pytest.mark.integration
    @patch("artifact_api.voxel.voxel_video.get_anonymized_path_from_raw", new=mock_anon)
    @patch("artifact_api.voxel.voxel_snapshot.get_anonymized_path_from_raw", new=mock_anon)
    def test_attach_rule_to_video(
            self,
            voxel_service: VoxelService,
            test_videos_artifacts: list[S3VideoArtifact]):
        """Test attach_rule_to_video"""
        # GIVEN
        dataset = fo.load_dataset("datanauts")
        rule1 = SelectorRule(rule_name="test", rule_version="1.0.0", origin=RuleOrigin.INTERIOR)
        rule2 = SelectorRule(rule_name="foo", rule_version="1.0.0", origin=RuleOrigin.INTERIOR)
        video_upload1 = VideoUploadRuleWithoutS3Validation(
            tenant="datanauts",
            raw_file_path=test_videos_artifacts[0].raw_s3_path,
            rule=rule1,
            video_id=test_videos_artifacts[0].artifact_id,
            footage_from=test_videos_artifacts[0].timestamp,
            footage_to=test_videos_artifacts[0].end_timestamp
        )
        video_upload2 = VideoUploadRuleWithoutS3Validation(
            tenant="datanauts",
            raw_file_path=test_videos_artifacts[0].raw_s3_path,
            rule=rule2,
            video_id=test_videos_artifacts[0].artifact_id,
            footage_from=test_videos_artifacts[0].timestamp,
            footage_to=test_videos_artifacts[0].end_timestamp,
        )
        # WHEN
        voxel_service.attach_rule_to_video(video_upload1)
        voxel_service.attach_rule_to_video(video_upload2)

        # THEN
        assert len(dataset) == 4
        rules = dataset.one(ViewField("raw_filepath").starts_with(test_videos_artifacts[0].raw_s3_path))["rules"]
        assert len(rules) == 2
        assert rules[0]["name"] == video_upload1.rule.rule_name
        assert rules[0]["version"] == video_upload1.rule.rule_version
        assert rules[0]["origin"] == video_upload1.rule.origin.value
        assert rules[0]["footage_from"] == video_upload1.footage_from
        assert rules[0]["footage_to"] == video_upload1.footage_to
        assert rules[1]["name"] == video_upload2.rule.rule_name

    @pytest.mark.integration
    @patch("artifact_api.voxel.voxel_video.get_anonymized_path_from_raw", new=mock_anon)
    @patch("artifact_api.voxel.voxel_snapshot.get_anonymized_path_from_raw", new=mock_anon)
    def test_attach_rule_to_snapshot(
            self,
            voxel_service: VoxelService,
            test_images_artifacts: list[S3VideoArtifact]):
        """Test attach_rule_to_video"""
        # GIVEN
        dataset = fo.load_dataset("datanauts_snapshots")
        rule1 = SelectorRule(rule_name="test_snap", rule_version="1.0.0", origin=RuleOrigin.INTERIOR)
        rule2 = SelectorRule(rule_name="foo_snap", rule_version="1.0.0", origin=RuleOrigin.INTERIOR)
        snap_upload1 = SnapshotUploadRuleWithoutS3Validation(
            tenant="datanauts",
            raw_file_path=test_images_artifacts[0].raw_s3_path,
            rule=rule1,
            snapshot_id="",
            snapshot_timestamp=test_images_artifacts[0].timestamp,
        )
        snap_upload2 = SnapshotUploadRuleWithoutS3Validation(
            tenant="datanauts",
            raw_file_path=test_images_artifacts[0].raw_s3_path,
            rule=rule2,
            snapshot_id="",
            snapshot_timestamp=test_images_artifacts[0].timestamp,
        )
        # WHEN
        voxel_service.attach_rule_to_snapshot(snap_upload1)
        voxel_service.attach_rule_to_snapshot(snap_upload2)

        # THEN
        assert len(dataset) == 4
        rules = dataset.one(ViewField("raw_filepath").starts_with(test_images_artifacts[0].raw_s3_path))["rules"]
        assert len(rules) == 2
        assert rules[0]["name"] == snap_upload1.rule.rule_name
        assert rules[0]["version"] == snap_upload1.rule.rule_version
        assert rules[0]["origin"] == snap_upload1.rule.origin.value
        assert rules[0]["snapshot_timestamp"] == snap_upload2.snapshot_timestamp
        assert rules[1]["name"] == snap_upload2.rule.rule_name
