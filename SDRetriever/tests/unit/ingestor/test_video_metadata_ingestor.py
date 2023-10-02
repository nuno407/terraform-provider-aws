from unittest.mock import Mock, call, ANY
from itertools import chain

import re
from pytest import fixture, mark, raises
from pytest_lazyfixture import lazy_fixture

from base.model.artifacts import (S3VideoArtifact, Artifact, SignalsArtifact)
from sdretriever.ingestor.video_metadata import VideoMetadataIngestor
from sdretriever.metadata_merger import MetadataMerger
from sdretriever.models import S3ObjectRCC, ChunkDownloadParamsByID, S3ObjectDevcloud
from sdretriever.s3.s3_downloader_uploader import S3DownloaderUploader
from sdretriever.s3.s3_chunk_downloader_rcc import RCCChunkDownloader


class TestVideoMetadataIngestor:

    @fixture()
    def video_signals_ingestor(
            self,
            rcc_chunk_downloader: RCCChunkDownloader,
            metadata_merger: MetadataMerger,
            s3_downloader_uploader: S3DownloaderUploader) -> VideoMetadataIngestor:
        return VideoMetadataIngestor(
            rcc_chunk_downloader,
            metadata_merger,
            s3_downloader_uploader
        )

    @fixture()
    def video_signals_artifact(
            self,
            training_video_artifact: S3VideoArtifact,
            mock_tenant_id: str,
            mock_device_id: str) -> SignalsArtifact:
        return SignalsArtifact(
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
    def test_other_artifacts_raise_error(self, artifact: Artifact,
                                         video_signals_ingestor: VideoMetadataIngestor):
        with raises(ValueError):
            video_signals_ingestor.ingest(artifact)

    @mark.unit()
    def test_ingest(
            self,
            video_signals_ingestor: VideoMetadataIngestor,
            rcc_chunk_downloader: RCCChunkDownloader,
            s3_downloader_uploader: S3DownloaderUploader,
            rcc_bucket: str,
            video_signals_artifact: SignalsArtifact,
            metadata_merger: MetadataMerger):
        # GIVEN
        list_chunk_recordings = video_signals_artifact.referred_artifact.recordings
        downloaded_chunks_mock = [
            [
                S3ObjectRCC(
                    data=b"metadata_data_mock",
                    s3_key=f"{chunk_id}",
                    bucket=rcc_bucket) for chunk_id in recording.chunk_ids] for recording in list_chunk_recordings]
        concatenated_data = list(chain.from_iterable(downloaded_chunks_mock))
        path_uploaded = "s3://bucket/metadata.json"
        metadata_merger.merge_metadata_chunks = Mock(return_value={})

        rcc_chunk_downloader.download_by_chunk_id = Mock(side_effect=downloaded_chunks_mock)
        s3_downloader_uploader.upload_to_devcloud_raw = Mock(return_value=path_uploaded)

        expected_calls = [call(
            ChunkDownloadParamsByID(
                recorder=video_signals_artifact.referred_artifact.recorder,
                recording_id=video_signals_artifact.referred_artifact.recordings[0].recording_id,
                chunk_ids=video_signals_artifact.referred_artifact.recordings[0].chunk_ids,
                device_id=video_signals_artifact.device_id,
                tenant=video_signals_artifact.tenant_id,
                start_search=video_signals_artifact.referred_artifact.timestamp,
                stop_search=ANY,
                suffixes=[".json.zip"])),
            call(ChunkDownloadParamsByID(
                recorder=video_signals_artifact.referred_artifact.recorder,
                recording_id=video_signals_artifact.referred_artifact.recordings[1].recording_id,
                chunk_ids=video_signals_artifact.referred_artifact.recordings[1].chunk_ids,
                device_id=video_signals_artifact.device_id,
                tenant=video_signals_artifact.tenant_id,
                start_search=video_signals_artifact.referred_artifact.timestamp,
                stop_search=ANY,
                suffixes=[".json.zip"]))
        ]

        expected_devcloud_object = S3ObjectDevcloud(
            data=b"{}",
            filename=f"{video_signals_artifact.artifact_id}.json",
            tenant=video_signals_artifact.tenant_id)

        # WHEN
        video_signals_ingestor.ingest(video_signals_artifact)

        # THEN
        assert rcc_chunk_downloader.download_by_chunk_id.call_count == len(list_chunk_recordings)
        rcc_chunk_downloader.download_by_chunk_id.assert_has_calls(expected_calls)
        s3_downloader_uploader.upload_to_devcloud_raw.assert_called_once_with(
            expected_devcloud_object)
        assert video_signals_artifact.s3_path == path_uploaded
        metadata_merger.merge_metadata_chunks.assert_called_once_with(concatenated_data)

    @mark.unit
    def test_is_already_ingested(self,
                                 video_signals_ingestor: VideoMetadataIngestor,
                                 s3_downloader_uploader: S3DownloaderUploader,
                                 video_signals_artifact: SignalsArtifact):
        # GIVEN
        is_file_return = Mock()
        s3_downloader_uploader.is_file_devcloud_raw = Mock(return_value=is_file_return)

        # WHEN
        result = video_signals_ingestor.is_already_ingested(video_signals_artifact)

        # THEN
        assert result == is_file_return
        s3_downloader_uploader.is_file_devcloud_raw.assert_called_once_with(
            video_signals_artifact.artifact_id + ".json", video_signals_artifact.tenant_id)
