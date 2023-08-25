from unittest.mock import Mock, call, ANY
from itertools import chain

import re
from pytest import fixture, mark, raises
from pytest_lazyfixture import lazy_fixture

from base.model.artifacts import (PreviewSignalsArtifact, Artifact, SignalsArtifact)
from sdretriever.ingestor.preview_metadata import PreviewMetadataIngestor
from sdretriever.metadata_merger import MetadataMerger
from sdretriever.models import S3ObjectRCC, ChunkDownloadParamsByPrefix, S3ObjectDevcloud
from sdretriever.s3.s3_downloader_uploader import S3DownloaderUploader
from sdretriever.s3.s3_chunk_downloader_rcc import RCCChunkDownloader


class TestPreviewMetadataIngestor:

    @fixture()
    def preview_metadata_ingestor(
            self,
            rcc_chunk_downloader: RCCChunkDownloader,
            metadata_merger: MetadataMerger,
            s3_downloader_uploader: S3DownloaderUploader) -> PreviewMetadataIngestor:
        return PreviewMetadataIngestor(
            rcc_chunk_downloader,
            s3_downloader_uploader,
            metadata_merger,
        )

    @mark.unit()
    @mark.parametrize("artifact", [
        (lazy_fixture("snapshot_artifact")),
        (lazy_fixture("interior_video_artifact")),
        (lazy_fixture("training_video_artifact"))
    ], ids=["fail_snapshot_artifact", "fail_interior_video_artifact", "fail_training_video_artifact"])
    def test_other_artifacts_raise_error(self, artifact: Artifact,
                                         preview_metadata_ingestor: PreviewMetadataIngestor):
        with raises(ValueError):
            preview_metadata_ingestor.ingest(artifact)

    @mark.unit()
    def test_ingest(
            self,
            preview_metadata_ingestor: PreviewMetadataIngestor,
            rcc_chunk_downloader: RCCChunkDownloader,
            s3_downloader_uploader: S3DownloaderUploader,
            preview_metadata_artifact: PreviewSignalsArtifact,
            metadata_merger: MetadataMerger):
        # GIVEN
        downloaded_chunks_mock = Mock()
        path_uploaded = "s3://metadata.json"
        chunks_uuids = [chunk.uuid for chunk in preview_metadata_artifact.referred_artifact.chunks]

        metadata_merger.merge_metadata_chunks = Mock(return_value={})
        rcc_chunk_downloader.download_by_prefix_suffix = Mock(return_value=downloaded_chunks_mock)
        s3_downloader_uploader.upload_to_devcloud_tmp = Mock(return_value=path_uploaded)

        expected_search_params = ChunkDownloadParamsByPrefix(
            device_id=preview_metadata_artifact.device_id,
            tenant=preview_metadata_artifact.tenant_id,
            start_search=preview_metadata_artifact.referred_artifact.timestamp,
            stop_search=ANY,
            suffixes=[
                ".json.zip",
                ".json"],
            files_prefix=chunks_uuids)

        expected_devcloud_object = S3ObjectDevcloud(
            data=b"{}",
            filename=f"{preview_metadata_artifact.artifact_id}.json",
            tenant=preview_metadata_artifact.tenant_id)

        # WHEN
        preview_metadata_ingestor.ingest(preview_metadata_artifact)

        # THEN
        rcc_chunk_downloader.download_by_prefix_suffix.assert_called_once_with(params=expected_search_params)
        metadata_merger.merge_metadata_chunks.assert_called_once_with(downloaded_chunks_mock)
        s3_downloader_uploader.upload_to_devcloud_tmp.assert_called_once_with(expected_devcloud_object)
        assert preview_metadata_artifact.s3_path == path_uploaded
