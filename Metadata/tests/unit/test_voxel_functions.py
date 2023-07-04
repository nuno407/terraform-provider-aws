from base.aws.s3 import S3Controller
from metadata.consumer.voxel.metadata_parser import MetadataParser
from base.voxel.voxel_snapshot_metadata_loader import VoxelSnapshotMetadataLoader
from metadata.consumer.config import DatasetMappingConfig
from metadata.consumer.voxel.functions import add_voxel_snapshot_metadata, get_voxel_snapshot_sample, get_voxel_sample_data_privacy_document_id
import pytest
from base.model.artifacts import SignalsArtifact, SnapshotArtifact, MetadataType, RecorderType, TimeWindow
from unittest.mock import Mock, patch, MagicMock, call
from datetime import datetime
from kink import di
import pytz


@pytest.mark.unit
class TestVoxelFunctions():

    @pytest.fixture()
    def tenant_id(self) -> str:
        """Tenant ID"""
        return "tenant_id"

    @pytest.fixture()
    def device_id(self) -> str:
        """Device ID"""
        return "device_id"

    @pytest.fixture()
    def snapshot_artifact(self, tenant_id: str, device_id: str) -> SnapshotArtifact:
        """SnapshotArtifact for testing."""
        return SnapshotArtifact(
            s3_path="s3://dev-rcd-video-raw/datanauts/datanauts_DATANAUTS_DEV_02_TrainingMultiSnapshot_TrainingMultiSnapshot-550caa7d-ef6a-4253-9400-5fc6c73fd693_1_1680704203713.jpeg",
            tenant_id=tenant_id,
            device_id=device_id,
            recorder=RecorderType.SNAPSHOT,
            timestamp=datetime(year=2022, month=12, day=12,
                               hour=1, minute=1, tzinfo=pytz.utc),
            upload_timing=TimeWindow(
                start=datetime(year=2023, month=1, day=1,
                               hour=1, minute=1, tzinfo=pytz.utc),
                end=datetime(year=2023, month=1, day=1, hour=1,
                             minute=2, tzinfo=pytz.utc),
            ),
            end_timestamp=datetime(year=2022, month=12, day=12,
                                   hour=1, minute=1, tzinfo=pytz.utc),
            uuid="SOME_ID"
        )

    @pytest.fixture()
    def snapshot_metadata_artifact(self, snapshot_artifact: SnapshotArtifact) -> SignalsArtifact:
        """SnapshotSignalsArtifact for testing."""
        return SignalsArtifact(
            s3_path="s3://dev-rcd-video-raw/datanauts/datanauts_DATANAUTS_DEV_02_TrainingMultiSnapshot_TrainingMultiSnapshot-550caa7d-ef6a-4253-9400-5fc6c73fd693_1_1680704203713_metadata.json",
            tenant_id=snapshot_artifact.tenant_id,
            device_id=snapshot_artifact.device_id,
            metadata_type=MetadataType.SIGNALS,
            referred_artifact=snapshot_artifact
        )

    @pytest.mark.unit
    @pytest.mark.parametrize("metadata_frames,file_exists,expected_exception", [
        (
            [],
            True,
            None
        ),
        (
            [Mock(), Mock()],
            True,
            ValueError
        ),
        (
            [Mock()],
            False,
            ValueError
        )
    ])
    @patch("metadata.consumer.voxel.functions.get_voxel_snapshot_sample")
    @patch("metadata.consumer.voxel.functions.json.loads")
    def test_add_voxel_snapshot_metadata_fail(self,
                                              json_loads: MagicMock,
                                              get_voxel_snapshot_sample_mock: MagicMock,
                                              metadata_frames: list,
                                              file_exists: bool,
                                              snapshot_metadata_artifact: SignalsArtifact,
                                              expected_exception: Exception,
                                              s3_controller: S3Controller,
                                              metadata_parser: MetadataParser,
                                              voxel_snapshot_metadata_loader: VoxelSnapshotMetadataLoader):

        # GIVEN
        sample_mock = Mock()
        metadata_mock = Mock()
        metadata_bucket, metadata_key = (Mock(), Mock())
        img_key = Mock()

        s3_controller.get_s3_path_parts = Mock(
            side_effect=[(metadata_bucket, metadata_key)])
        s3_controller.check_s3_file_exists = Mock(return_value=file_exists)

        get_voxel_snapshot_sample_mock.return_value = sample_mock

        s3_controller.download_file = Mock(return_value=b"{}")
        json_loads.return_value = metadata_mock
        metadata_parser.parse = Mock(return_value=metadata_frames)
        voxel_snapshot_metadata_loader.set_sample = Mock()
        voxel_snapshot_metadata_loader.load = Mock()

        sample_mock.save = Mock()

        # THEN-WHEN
        if expected_exception is not None:
            with pytest.raises(expected_exception):
                add_voxel_snapshot_metadata(
                    snapshot_metadata_artifact,
                    s3_controller,
                    metadata_parser,
                    voxel_snapshot_metadata_loader)
            return
        add_voxel_snapshot_metadata(
            snapshot_metadata_artifact,
            s3_controller,
            metadata_parser,
            voxel_snapshot_metadata_loader)

        # THEN
        s3_controller.get_s3_path_parts.assert_called_once_with(
            snapshot_metadata_artifact.s3_path)
        get_voxel_snapshot_sample_mock.assert_called_once_with(
            snapshot_metadata_artifact.tenant_id, snapshot_metadata_artifact.referred_artifact.artifact_id)
        s3_controller.check_s3_file_exists.assert_called_once_with(
            metadata_bucket, metadata_key)
        s3_controller.download_file.assert_called_once_with(
            metadata_bucket, metadata_key)

        metadata_parser.parse.assert_called_once_with(metadata_mock)
        if len(metadata_frames):
            voxel_snapshot_metadata_loader.set_sample.assert_called_once_with(
                sample_mock)
            voxel_snapshot_metadata_loader.load.assert_called_once_with(
                metadata_frames[0])
            sample_mock.save.assert_called()

    @pytest.mark.parametrize("tenant,snap_id,expected_dataset_name", [
        (
            "datanauts",
            "some_id",
            "RC-datanauts_snapshots"

        ),
        (
            "non-eixstent",
            "some_id",
            "Debug_Lync_snapshots"
        ),
    ])
    def test_get_voxel_snapshot_sample(
            self,
            dataset_config: DatasetMappingConfig,
            fiftyone: MagicMock,
            tenant: str,
            snap_id: str,
            expected_dataset_name: str):

        # GIVEN
        dataset = Mock()
        fiftyone.load_dataset.return_value = dataset
        return_mock = MagicMock()
        dataset.one.return_value = return_mock
        di[DatasetMappingConfig] = dataset_config

        # WHEN
        result_data = get_voxel_snapshot_sample(tenant, snap_id)

        # THEN
        fiftyone.load_dataset.assert_called_once_with(expected_dataset_name)
        assert result_data == return_mock

    @pytest.mark.unit
    @pytest.mark.parametrize("s3_path,expected_policy",
                             [("s3://dev-rcd-video-raw/datanauts/datanauts_DATANAUTS_DEV_02_TrainingMultiSnapshot_TrainingMultiSnapshot.jpeg",
                               "default-policy"),
                              ("s3://dev-rcd-video-raw/test-tenant/test_tenant_DATANAUTS_DEV_02_TrainingMultiSnapshot_TrainingMultiSnapshot.jpeg",
                               "test-policy"),
                              ])
    def test_get_voxel_sample_data_privacy_document_id(
            self,
            dataset_config: DatasetMappingConfig,
            s3_path: str,
            expected_policy: str):
        # GIVEN
        sample_mock = Mock()
        sample_mock.filepath = s3_path
        di[DatasetMappingConfig] = dataset_config
        # WHEN
        policy = get_voxel_sample_data_privacy_document_id(sample_mock)
        assert policy == expected_policy
