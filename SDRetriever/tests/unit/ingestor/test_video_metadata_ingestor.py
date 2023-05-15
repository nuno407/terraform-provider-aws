import json
from datetime import datetime, timedelta
from unittest.mock import Mock, PropertyMock

from pytest import fixture, mark, raises
from pytz import UTC

from base.aws.container_services import ContainerServices
from base.aws.s3 import S3ClientFactory, S3Controller
from base.model.artifacts import (RecorderType, SignalsArtifact,
                                  SnapshotArtifact, TimeWindow)
from sdretriever.exceptions import UploadNotYetCompletedError
from sdretriever.ingestor.metacontent import MetacontentChunk
from sdretriever.ingestor.video_metadata import VideoMetadataIngestor
from sdretriever.s3_finder import S3Finder

RAW_S3 = "raw-s3"
CHUNK_PATHS = [
    "file1.json",
    "file2.json",
]
CHUNKS = [
    MetacontentChunk(filename=CHUNK_PATHS[0], data=b"""
    {
        "chunk": {
            "pts_start": 0,
            "pts_end": 1000
        },
        "chunk": {
            "utc_start": 100,
            "utc_end": 200
        },
        "resolution": "640x480",
        "frame": [
            {
                "number": 4,
                "foo": "bar"
            },
            {
                "number": 2,
                "foo": "baz"
            }
        ]
    }"""),
    MetacontentChunk(filename=CHUNK_PATHS[1], data=b"""
    {
        "chunk": {
            "pts_start": 1000,
            "pts_end": 2000,
            "utc_start": 200,
            "utc_end": 300
        },
        "frame": [
            {
                "number": 1,
                "foo": "bar"
            },
            {
                "number": 3,
                "foo": "baz"
            }
        ]
    }""")
]
SNAP_TIME = datetime.now(tz=UTC) - timedelta(hours=4)
UPLOAD_START = datetime.now(tz=UTC) - timedelta(hours=2)
UPLOAD_END = datetime.now(tz=UTC) - timedelta(hours=1)
TENANT_ID = "foo"
DEVICE_ID = "bar"
UID = "baz"
ART_ID = f"{TENANT_ID}_{DEVICE_ID}_{UID}_{round(SNAP_TIME.timestamp()*1000)}_metadata_full"


class TestMetadataIngestor:
    @fixture()
    def container_services(self) -> ContainerServices:
        container_services = Mock()
        type(container_services).raw_s3 = PropertyMock(return_value=RAW_S3)
        return container_services

    @fixture()
    def rcc_client_factory(self) -> S3ClientFactory:
        return Mock()

    @fixture()
    def s3_controller(self) -> S3Controller:
        return Mock()

    @fixture()
    def s3_finder(self) -> S3Finder:
        return Mock()

    @fixture()
    def metadata_ingestor(
            self,
            container_services: ContainerServices,
            rcc_client_factory: S3ClientFactory,
            s3_controller: S3Controller,
            s3_finder: S3Finder) -> VideoMetadataIngestor:
        return VideoMetadataIngestor(
            container_services,
            rcc_client_factory,
            s3_controller,
            s3_finder
        )

    @fixture()
    def snapshot_artifact(self) -> SnapshotArtifact:
        return SnapshotArtifact(
            tenant_id=TENANT_ID,
            device_id=DEVICE_ID,
            uuid=UID,
            recorder=RecorderType.SNAPSHOT,
            upload_timing=TimeWindow(
                start=UPLOAD_START,
                end=UPLOAD_END
            ),
            timestamp=SNAP_TIME
        )

    @fixture()
    def metadata_artifact(self, snapshot_artifact: SnapshotArtifact) -> SignalsArtifact:
        return SignalsArtifact(
            tenant_id=TENANT_ID,
            device_id=DEVICE_ID,
            referred_artifact=snapshot_artifact
        )

    @mark.unit()
    def test_other_artifacts_raise_error(self, snapshot_artifact: SnapshotArtifact,
                                         metadata_ingestor: VideoMetadataIngestor):
        with raises(ValueError):
            metadata_ingestor.ingest(snapshot_artifact)

    @mark.unit()
    def test_raise_if_not_all_parts_exist(self,
                                          metadata_artifact: SignalsArtifact,
                                          metadata_ingestor: VideoMetadataIngestor):
        # GIVEN
        metadata_ingestor._check_allparts_exist = Mock(return_value=(False, []))  # type: ignore[method-assign]

        # WHEN
        with raises(UploadNotYetCompletedError):
            metadata_ingestor.ingest(metadata_artifact)

    @mark.unit()
    def test_successful_ingestion(self, metadata_artifact: SignalsArtifact, metadata_ingestor: VideoMetadataIngestor):
        # GIVEN
        metadata_ingestor._check_allparts_exist = Mock(return_value=(True, CHUNK_PATHS))  # type: ignore[method-assign]
        metadata_ingestor._get_metacontent_chunks = Mock(return_value=CHUNKS)  # type: ignore[method-assign]
        metadata_ingestor._upload_metacontent_to_devcloud = Mock(  # type: ignore[method-assign]
            return_value=f"s3://{RAW_S3}/{metadata_artifact.tenant_id}/{metadata_artifact.artifact_id}.json")

        # WHEN
        metadata_ingestor.ingest(metadata_artifact)

        # THEN
        metadata_ingestor._check_allparts_exist.assert_called_once_with(metadata_artifact)
        metadata_ingestor._get_metacontent_chunks.assert_called_once_with(CHUNK_PATHS)
        uploaded: dict = json.loads(metadata_ingestor._upload_metacontent_to_devcloud.call_args.args[0].data)

        assert uploaded["chunk"]["pts_start"] == 0
        assert uploaded["chunk"]["pts_end"] == 2000
        assert uploaded["chunk"]["utc_start"] == 100
        assert uploaded["chunk"]["utc_end"] == 300
        assert uploaded["resolution"] == "640x480"
        assert [1, 2, 3, 4] == [frame["number"] for frame in uploaded["frame"]]
        assert ["bar", "baz", "baz", "bar"] == [frame["foo"] for frame in uploaded["frame"]]

        assert metadata_artifact.s3_path == f"s3://{RAW_S3}/{TENANT_ID}/{ART_ID}.json"
