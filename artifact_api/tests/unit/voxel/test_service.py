"Unit tests for voxel service"
from unittest.mock import Mock, MagicMock, call, ANY
from datetime import datetime, timezone, timedelta
import fiftyone.core.media as fom
import fiftyone as fo
import pytest
from pytest_mock import MockerFixture
from kink import di
from base.model.config.dataset_config import DatasetConfig
from base.model.config.policy_config import PolicyConfig
from base.voxel.functions import get_anonymized_path_from_raw
from base.model.artifacts import S3VideoArtifact, SnapshotArtifact, RecorderType, TimeWindow, Resolution, SnapshotSignalsData, VideoSignalsData
from artifact_api.voxel.service import VoxelService
from artifact_api.voxel.voxel_config import VoxelConfig

# pylint: disable=duplicate-code


class TestVoxelService:
    "Unit tests for controller endpoints"

    @pytest.fixture(name="dataset_mapping")
    def fixture_dataset_mapping(self) -> DatasetConfig:
        """generate DatasetConfig
        """
        return DatasetConfig(default_dataset="datanauts", tag="RC")

    @pytest.fixture(name="voxel_service")
    def fixture_generate_voxel_service(self, mock_voxel_metadata_transformer: Mock) -> VoxelService:
        """generate VoxelService
        """
        dataset_config = DatasetConfig(default_dataset="datanauts", tag="RC")
        policy_config = PolicyConfig(default_policy_document="test_doc")
        voxel_config = VoxelConfig(dataset_mapping=dataset_config, policy_mapping=policy_config)
        di[PolicyConfig] = policy_config
        return VoxelService(voxel_config=voxel_config, metadata_transformer=mock_voxel_metadata_transformer)

    @pytest.fixture(name="mock_image_build_for_sample")
    def fixture_image_build_for_sample(self, mocker: MockerFixture) -> Mock:
        """mocks image_build_for_sample function
        """
        return mocker.patch("artifact_api.voxel.voxel_base_models.ImageMetadata.build_for")

    @pytest.fixture(name="mock_video_build_for_sample")
    def fixture_video_build_for_sample(self, mocker: MockerFixture) -> Mock:
        """mocks video_build_for_sample function
        """
        return mocker.patch("artifact_api.voxel.voxel_base_models.VideoMetadata.build_for")

    @pytest.fixture(name="mock_create_dataset")
    def fixture_create_dataset(self, mocker: MockerFixture) -> Mock:
        """mocks create_dataset function
        """
        return mocker.patch("artifact_api.voxel.service.create_dataset")

    @pytest.fixture(name="mock_find_or_create_sample")
    def fixture_find_or_create_sample(self, mocker: MockerFixture) -> Mock:
        """mocks find_or_create_sample function
        """
        return mocker.patch("artifact_api.voxel.voxel_base_models.find_or_create_sample")

    @pytest.fixture(name="mock_set_field")
    def fixture_set_field(self, mocker: MockerFixture) -> Mock:
        """mocks set_field function
        """
        return mocker.patch("artifact_api.voxel.voxel_base_models.set_field")

    @pytest.fixture(name="mock_set_mandatory_fields_on_sample")
    def fixture_set_mandatory_fields_on_sample(self, mocker: MockerFixture) -> Mock:
        """mocks set_mandatory_fields_on_sample function
        """
        return mocker.patch("artifact_api.voxel.voxel_base_models.set_mandatory_fields_on_sample")

    @pytest.fixture(name="mock_load_snapshot_metadata")
    def fixture_load_snapshot_metadata(self, mocker: MockerFixture) -> Mock:
        """mocks load_snapshot_metadata function
        """
        return mocker.patch("artifact_api.voxel.service.VoxelSnapshot.load_metadata")

    @pytest.fixture()
    def video_artifact(self) -> S3VideoArtifact:
        """VideoArtifact for testing."""
        return S3VideoArtifact(
            artifact_id="bar",
            raw_s3_path="s3://dev-rcd-video-raw/datanauts/test123.mp4",
            anonymized_s3_path="s3://dev-rcd-video-anonymized/datanauts/test123_anonymized.mp4",
            rcc_s3_path="s3://dev-rcd-video-raw/datanauts/test123.mp4",
            s3_path="s3://dev-rcd-video-raw/datanauts/test123.mp4",
            tenant_id="datanauts",
            device_id="DATANAUTS_DEV_01",
            recorder=RecorderType.INTERIOR,
            timestamp=datetime.now(tz=timezone.utc),
            end_timestamp=datetime.now(tz=timezone.utc),
            upload_timing=TimeWindow(start=(datetime.now() - timedelta(minutes=50)), end=datetime.now()),
            footage_id="footage_id",
            recordings=[],
            resolution=Resolution(width=11, height=12)
        )

    @pytest.fixture()
    def snapshot_artifact(self) -> SnapshotArtifact:
        """SnapshotArtifact for testing."""
        return SnapshotArtifact(
            artifact_id="bar",
            raw_s3_path="s3://dev-rcd-video-raw/datanauts/test123.jpeg",
            anonymized_s3_path="s3://dev-rcd-video-anonymized/datanauts/test123_anonymized.jpeg",
            s3_path="s3://dev-rcd-video-raw/datanauts/test123.jpeg",
            tenant_id="datanauts",
            device_id="DATANAUTS_DEV_02",
            recorder=RecorderType.SNAPSHOT,
            timestamp=datetime.now(tz=timezone.utc),
            end_timestamp=datetime.now(tz=timezone.utc),
            upload_timing=TimeWindow(start=(datetime.now() - timedelta(minutes=5)), end=datetime.now()),
            uuid="uuid",
            resolution=Resolution(width=11, height=12))

    @pytest.mark.unit
    def test_voxel_video(self, video_artifact: S3VideoArtifact,  # pylint: disable=too-many-arguments
                         mock_create_dataset: Mock,
                         mock_find_or_create_sample: Mock,
                         mock_set_field: Mock,
                         mock_set_mandatory_fields_on_sample: Mock,
                         mock_video_build_for_sample: Mock,
                         voxel_service: VoxelService):
        """
        Test a video sample create and update.

        Args:
            artifact (S3VideoArtifact): _description_
            dataset (MagicMock): _description_
        """
        # GIVEN
        raw_metadata = Mock()
        dataset = MagicMock()
        mock_create_dataset.return_value = dataset
        sample = MagicMock()
        correlated_raw_filepaths = []
        mock_video_build_for_sample.return_value = raw_metadata
        mock_find_or_create_sample.return_value = sample
        sample.media_type = fom.VIDEO
        set_field_calls = [
            call(sample, "video_id", video_artifact.artifact_id),
            call(sample, "tenant_id", video_artifact.tenant_id),
            call(sample, "device_id", video_artifact.device_id),
            call(sample, "recording_time", video_artifact.timestamp),
            call(sample, "hour", video_artifact.timestamp.hour),
            call(sample, "day", video_artifact.timestamp.day),
            call(sample, "month", video_artifact.timestamp.month),
            call(sample, "year", video_artifact.timestamp.year),
            call(sample, "recording_duration", video_artifact.duration),
            call(sample, "resolution", f"{video_artifact.resolution.width}x{video_artifact.resolution.height}"),
            call(sample, "snapshots_paths", ANY),
            call(sample, "num_snapshots", len(correlated_raw_filepaths)),
            call(sample, "raw_metadata", raw_metadata),  # Compute raw_metadata
        ]
        # WHEN
        voxel_service.create_voxel_video(video_artifact, correlated_raw_filepaths)
        # THEN
        mock_create_dataset.assert_called_once_with("datanauts", ["RC"])
        mock_find_or_create_sample.assert_called_once_with(
            dataset, get_anonymized_path_from_raw(
                video_artifact.s3_path))
        mock_set_field.assert_has_calls(set_field_calls)
        assert mock_set_field.call_count == len(set_field_calls)
        mock_set_mandatory_fields_on_sample.assert_called_once_with(sample, video_artifact.tenant_id)
        sample.compute_metadata.assert_called_once()
        sample.save.assert_called_once()

    @pytest.mark.unit
    def test_voxel_snapshot(self, snapshot_artifact: SnapshotArtifact,  # pylint: disable=too-many-arguments
                            mock_create_dataset: Mock,
                            mock_find_or_create_sample: Mock,
                            mock_set_field: Mock,
                            mock_set_mandatory_fields_on_sample: Mock,
                            mock_image_build_for_sample: Mock,
                            voxel_service: VoxelService):
        """
        Test a snapshot sample create and update.

        Args:
            snapshot_artifact (SnapshotArtifact): _description_
            dataset (MagicMock): _description_
        """
        # GIVEN
        raw_metadata = Mock()
        dataset = MagicMock()
        mock_create_dataset.return_value = dataset
        sample = MagicMock()
        correlated_raw_filepaths = []
        mock_image_build_for_sample.return_value = raw_metadata
        mock_find_or_create_sample.return_value = sample
        sample.media_type = fom.IMAGE
        set_field_calls = [
            call(sample, "video_id", snapshot_artifact.artifact_id),
            call(sample, "tenant_id", snapshot_artifact.tenant_id),
            call(sample, "device_id", snapshot_artifact.device_id),
            call(sample, "source_videos", correlated_raw_filepaths),
            call(sample, "recording_time", snapshot_artifact.timestamp),
            call(sample, "raw_metadata", raw_metadata)  # Compute raw_metadata
        ]
        # WHEN
        voxel_service.create_voxel_snapshot(snapshot_artifact, correlated_raw_filepaths)

        # THEN
        mock_create_dataset.assert_called_once_with("datanauts_snapshots", ["RC"])
        mock_find_or_create_sample.assert_called_once_with(
            dataset, get_anonymized_path_from_raw(
                snapshot_artifact.s3_path))
        mock_set_field.assert_has_calls(set_field_calls, True)
        assert mock_set_field.call_count == len(set_field_calls)
        mock_set_mandatory_fields_on_sample.assert_called_once_with(sample, snapshot_artifact.tenant_id)
        sample.compute_metadata.assert_called_once()
        sample.save.assert_called_once()

    @pytest.mark.unit
    def test_load_snapshot_metadata(
            self,
            voxel_service: VoxelService,
            mock_create_dataset: Mock,
            mock_load_snapshot_metadata: Mock,
            mock_voxel_metadata_transformer: Mock,
            snap_signals_artifact: SnapshotSignalsData,
            dataset_mapping: DatasetConfig):
        """
        Test load_snapshot_metadata function.

        Args:
            voxel_service (VoxelService): _description_
            fixture_create_dataset (Mock): _description_
            mock_load_snapshot_metadata (Mock): _description_
            mock_voxel_metadata_transformer (Mock): _description_
        """
        # GIVEN
        dataset_name = "datanauts_snapshots"
        dataset_tag = ["RC"]

        created_dataset = Mock()

        mock_create_dataset.return_value = created_dataset
        voxel_fields = Mock()
        mock_voxel_metadata_transformer.transform_snapshot_metadata_to_voxel.return_value = voxel_fields
        # WHEN
        voxel_service.load_snapshot_metadata(snap_signals_artifact)
        # THEN
        mock_create_dataset.assert_called_once_with(dataset_name, dataset_tag)
        mock_voxel_metadata_transformer.transform_snapshot_metadata_to_voxel.assert_called_once_with(
            snap_signals_artifact.data)
        mock_load_snapshot_metadata.assert_called_once_with(
            created_dataset,
            snap_signals_artifact.message.referred_artifact.anonymized_s3_path,
            snap_signals_artifact.message.tenant_id,
            voxel_fields)

    @pytest.mark.unit
    def test_load_device_video_aggregated_metadata(self, aggregated_metadata: dict[str, str | int | float | bool],  # pylint: disable=too-many-arguments
                                                   mock_create_dataset: Mock,
                                                   mock_find_or_create_sample: Mock,
                                                   mock_set_field: Mock,
                                                   mock_set_mandatory_fields_on_sample: Mock,
                                                   mock_video_build_for_sample: Mock,
                                                   voxel_service: VoxelService):
        """
        Test a snapshot sample create and update.

        Args:
            snapshot_artifact (SnapshotArtifact): _description_
            dataset (MagicMock): _description_
        """
        # GIVEN
        artifact = VideoSignalsData(
            data={},
            aggregated_metadata=aggregated_metadata,
            correlation_id="correlation_id",
            tenant_id="datanauts",
            video_raw_s3_path="s3://dev-rcd-video-raw/datanauts/test123.mp4")
        dynamic_doc = fo.DynamicEmbeddedDocument(**aggregated_metadata)
        raw_metadata = Mock()
        dataset = MagicMock()
        mock_create_dataset.return_value = dataset
        sample = MagicMock()
        mock_video_build_for_sample.return_value = raw_metadata
        mock_find_or_create_sample.return_value = sample
        sample.media_type = fom.VIDEO
        set_field_calls = [
            call(sample, "aggregated_metadata", dynamic_doc),
            call(sample, "raw_metadata", raw_metadata)  # Compute raw_metadata
        ]
        # WHEN
        voxel_service.load_device_video_aggregated_metadata(artifact)

        # THEN
        mock_create_dataset.assert_called_once_with("datanauts", ["RC"])
        mock_find_or_create_sample.assert_called_once_with(
            dataset, get_anonymized_path_from_raw(artifact.video_raw_s3_path))
        mock_set_field.assert_has_calls(set_field_calls, True)
        assert mock_set_field.call_count == len(set_field_calls)
        mock_set_mandatory_fields_on_sample.assert_called_once_with(sample, artifact.tenant_id)
        sample.compute_metadata.assert_called_once()
        sample.save.assert_called_once()
