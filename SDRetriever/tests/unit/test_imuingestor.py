# type: ignore
from unittest.mock import ANY, Mock

import pytest

from typing import Optional
from sdretriever.ingestor.metacontent import MetacontentChunk
from sdretriever.message.video import VideoMessage
from sdretriever.ingestor.imu import IMUIngestor, IMU_FILE_EXT


@pytest.mark.unit
@pytest.mark.usefixtures("imu_files", "imu_ingestor", "imu_chunks", "imu_full", "training_message_metadata")
class TestIMUIngestor:

    @pytest.mark.unit
    @pytest.mark.parametrize("chunk_list,expected_concat", [
        (
            pytest.lazy_fixture("imu_chunks"),
            pytest.lazy_fixture("imu_full")
        ),
        (
            [
                MetacontentChunk(bytes([1, 2, 0, 0, 1, 10]), "some_file"),
                MetacontentChunk(bytes([10, 2, 3, 9, 3, 120]), "some_file1"),
            ],
            bytes([1, 2, 0, 0, 1, 10, 10, 2, 3, 9, 3, 120])
        )
    ])
    def test_concatenate_chunks(
            self,
            chunk_list: list[MetacontentChunk],
            expected_concat: bytearray,
            imu_ingestor: IMUIngestor):
        assert imu_ingestor.concatenate_chunks(chunk_list) == expected_concat

    @pytest.mark.unit
    @pytest.mark.parametrize("chunk_list,expected_concat", [
        (
            pytest.lazy_fixture("imu_chunks"),
            pytest.lazy_fixture("imu_full")
        ),
        (
            [
                MetacontentChunk(bytes([1, 2, 0, 0, 1, 10]), "some_file"),
                MetacontentChunk(bytes([10, 2, 3, 9, 3, 120]), "some_file1"),
            ],
            bytes([1, 2, 0, 0, 1, 10, 10, 2, 3, 9, 3, 120])
        )
    ])
    def test_concatenate_chunks2(
            self,
            chunk_list: list[MetacontentChunk],
            expected_concat: bytearray,
            imu_ingestor: IMUIngestor):
        assert imu_ingestor.concatenate_chunks(chunk_list) == expected_concat

    @pytest.mark.unit
    @pytest.mark.parametrize("video_msg,video_id,imu_files,return_expected",
                             [(pytest.lazy_fixture("training_message_metadata"),
                               "some_video_id",
                               {"TrainingRecorder_TrainingRecorder-7d5d57c3-aa1d-4554-8d44-7a3e13227749_56.mp4._stream1_20230221171736_153_imu_raw.csv.zip",
                                "TrainingRecorder_TrainingRecorder-7d5d57c3-aa1d-4554-8d44-7a3e13227749_57.mp4._stream1_20230221171748_154_imu_raw.csv.zip",
                                "TrainingRecorder_TrainingRecorder-7d5d57c3-aa1d-4554-8d44-7a3e13227749_58.mp4._stream1_20230221171759_155_imu_raw.csv.zip"},
                                 f"some_video_id.{IMU_FILE_EXT}"),
                              ])
    def test_ingest(
            self,
            video_msg: VideoMessage,
            video_id: str,
            imu_files: set[str],
            return_expected: Optional[str],
            imu_ingestor: IMUIngestor):

        file_merged = Mock()
        path = f"{video_msg.tenant}/{video_id}{IMU_FILE_EXT}"
        meta_chunks = [MetacontentChunk(Mock(), file) for file in imu_files]
        imu_ingestor._get_metacontent_chunks = Mock(return_value=meta_chunks)
        imu_ingestor.container_svcs.download_file = Mock(return_value="MOCKED_DATA")
        IMUIngestor.concatenate_chunks = Mock(return_value=file_merged)

        result_path: str = imu_ingestor.ingest(video_msg, video_id, imu_files)

        imu_ingestor.container_svcs.upload_file.assert_called_once_with(
            ANY, file_merged, imu_ingestor.container_svcs.raw_s3, path)
        assert result_path == path
