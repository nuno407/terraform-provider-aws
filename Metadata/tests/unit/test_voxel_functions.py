from base.aws.s3 import S3Controller
from metadata.consumer.voxel.metadata_parser import MetadataParser
from metadata.consumer.voxel.voxel_metadata_loader import VoxelSnapshotMetadataLoader
from metadata.consumer.voxel.functions import add_voxel_snapshot_metadata, get_voxel_sample
import pytest
from unittest.mock import Mock, patch, MagicMock, call, PropertyMock


@pytest.mark.unit
class TestVoxelFunctions():
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
    @patch("metadata.consumer.voxel.functions.get_voxel_sample")
    @patch("metadata.consumer.voxel.functions.json.loads")
    def test_add_voxel_snapshot_metadata_fail(self,
                                              json_loads: MagicMock,
                                              get_voxel_sample_mock: MagicMock,
                                              metadata_frames: list,
                                              file_exists: bool,
                                              expected_exception: Exception,
                                              s3_controller: S3Controller,
                                              metadata_parser: MetadataParser,
                                              voxel_snapshot_metadata_loader: VoxelSnapshotMetadataLoader):

        # GIVEN
        snapshot_id = "datanauts_DATANAUTS_DEV_02_TrainingMultiSnapshot_TrainingMultiSnapshot-550caa7d-ef6a-4253-9400-5fc6c73fd693_1_1680704203713"
        snapshot_path = "s3://dev-rcd-video-raw/datanauts/datanauts_DATANAUTS_DEV_02_TrainingMultiSnapshot_TrainingMultiSnapshot-550caa7d-ef6a-4253-9400-5fc6c73fd693_1_1680704203713.jpeg"
        metadata_path = "s3://dev-rcd-video-raw/datanauts/datanauts_DATANAUTS_DEV_02_TrainingMultiSnapshot_TrainingMultiSnapshot-550caa7d-ef6a-4253-9400-5fc6c73fd693_1_1680704203713_metadata.json"
        sample_mock = Mock()
        metadata_mock = Mock()
        metadata_bucket, metadata_key = (Mock(), Mock())
        img_key = Mock()

        s3_controller.get_s3_path_parts = Mock(
            side_effect=[(metadata_bucket, metadata_key), (Mock(), img_key)])
        s3_controller.check_s3_file_exists = Mock(return_value=file_exists)

        get_voxel_sample_mock.return_value = sample_mock

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
                    snapshot_id,
                    snapshot_path,
                    metadata_path,
                    s3_controller,
                    metadata_parser,
                    voxel_snapshot_metadata_loader)
            return
        add_voxel_snapshot_metadata(
            snapshot_id,
            snapshot_path,
            metadata_path,
            s3_controller,
            metadata_parser,
            voxel_snapshot_metadata_loader)

        # THEN
        s3_controller.get_s3_path_parts.assert_has_calls(
            [call(metadata_path), call(snapshot_path)], any_order=True)

        get_voxel_sample_mock.assert_called_once_with(img_key, snapshot_id)
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

    @patch("metadata.consumer.voxel.functions._determine_dataset_name")
    def test_get_voxel_sample(self, determine_dataset_name_mock: Mock, fiftyone: MagicMock):

        # GIVEN
        key = "some_key"
        id = "some_id"

        dataset_name = Mock()
        determine_dataset_name_mock.return_value = (
            dataset_name, Mock)
        dataset = MagicMock()
        fiftyone.load_dataset.return_value = dataset
        return_mock = MagicMock()
        dataset.one.return_value = return_mock

        # WHEN
        result_data = get_voxel_sample(key, id)

        # THEN
        determine_dataset_name_mock.assert_called_once_with(key)
        fiftyone.load_dataset.assert_called_once_with(dataset_name)
        assert result_data == return_mock
