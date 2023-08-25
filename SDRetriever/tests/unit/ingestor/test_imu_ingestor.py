from unittest.mock import Mock, call, ANY
from itertools import chain

import re
from pytest import fixture, mark, raises
from pytest_lazyfixture import lazy_fixture

from base.model.artifacts import (IMUArtifact, S3VideoArtifact, SnapshotArtifact)
from sdretriever.ingestor.imu import IMUIngestor
from sdretriever.models import S3ObjectRCC, ChunkDownloadParamsByID, S3ObjectDevcloud
from sdretriever.s3.s3_downloader_uploader import S3DownloaderUploader
from sdretriever.s3.s3_chunk_downloader_rcc import RCCChunkDownloader


class TestImuIngestor:

    @fixture()
    def imu_ingestor(
            self,
            rcc_chunk_downloader: RCCChunkDownloader,
            s3_downloader_uploader: S3DownloaderUploader) -> IMUIngestor:
        return IMUIngestor(
            rcc_chunk_downloader,
            s3_downloader_uploader
        )

    @fixture()
    def imu_artifact(
            self,
            training_video_artifact: S3VideoArtifact,
            mock_tenant_id: str,
            mock_device_id: str) -> IMUArtifact:
        return IMUArtifact(
            tenant_id=mock_tenant_id,
            device_id=mock_device_id,
            referred_artifact=training_video_artifact
        )

    @mark.unit()
    @mark.parametrize("artifact",
                      [(lazy_fixture("snapshot_artifact")),
                       (lazy_fixture("interior_video_artifact")),
                          (lazy_fixture("training_video_artifact")),
                          (lazy_fixture("preview_metadata_artifact"))],
                      ids=["fail_snapshot_artifact",
                           "fail_interior_video_artifact",
                           "fail_training_video_artifact",
                           "fail_preview_video_artifact"])
    def test_other_artifacts_raise_error(self, artifact: SnapshotArtifact,
                                         imu_ingestor: IMUIngestor):
        with raises(ValueError):
            imu_ingestor.ingest(artifact)

    @mark.unit()
    def test_successful_ingestion(
            self,
            imu_artifact: IMUArtifact,
            imu_ingestor: IMUIngestor,
            rcc_chunk_downloader: RCCChunkDownloader,
            s3_downloader_uploader: S3DownloaderUploader,
            rcc_bucket: str):
        # GIVEN
        list_chunk_recordings = imu_artifact.referred_artifact.recordings
        downloaded_chunks_mock = [
            [
                S3ObjectRCC(
                    data=b"imu_data_mock",
                    s3_key=f"{chunk_id}",
                    bucket=rcc_bucket) for chunk_id in recording.chunk_ids] for recording in list_chunk_recordings]
        concatenated_data = b"".join(list(map(lambda x: x.data, chain.from_iterable(downloaded_chunks_mock))))
        path_uploaded = "s3://imu.csv"

        rcc_chunk_downloader.download_by_chunk_id = Mock(side_effect=downloaded_chunks_mock)
        s3_downloader_uploader.upload_to_devcloud_raw = Mock(return_value=path_uploaded)

        expected_calls = [call(
            ChunkDownloadParamsByID(
                recorder=imu_artifact.referred_artifact.recorder,
                recording_id=imu_artifact.referred_artifact.recordings[0].recording_id,
                chunk_ids=imu_artifact.referred_artifact.recordings[0].chunk_ids,
                device_id=imu_artifact.device_id,
                tenant=imu_artifact.tenant_id,
                start_search=imu_artifact.referred_artifact.timestamp,
                stop_search=ANY,
                suffixes=["imu_raw.csv.zip"])),
            call(ChunkDownloadParamsByID(
                recorder=imu_artifact.referred_artifact.recorder,
                recording_id=imu_artifact.referred_artifact.recordings[1].recording_id,
                chunk_ids=imu_artifact.referred_artifact.recordings[1].chunk_ids,
                device_id=imu_artifact.device_id,
                tenant=imu_artifact.tenant_id,
                start_search=imu_artifact.referred_artifact.timestamp,
                stop_search=ANY,
                suffixes=["imu_raw.csv.zip"]))
        ]

        expected_devcloud_object = S3ObjectDevcloud(
            data=concatenated_data,
            filename=f"{imu_artifact.artifact_id}.csv",
            tenant=imu_artifact.tenant_id)

        # WHEN
        imu_ingestor.ingest(imu_artifact)

        # THEN
        assert rcc_chunk_downloader.download_by_chunk_id.call_count == len(list_chunk_recordings)
        rcc_chunk_downloader.download_by_chunk_id.assert_has_calls(expected_calls)
        s3_downloader_uploader.upload_to_devcloud_raw.assert_called_once_with(expected_devcloud_object)
        assert imu_artifact.s3_path == path_uploaded
