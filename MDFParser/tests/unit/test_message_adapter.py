""" Unit tests for the Downloader class. """
from datetime import datetime

from pytest import mark

from base.model.artifacts import (IMUArtifact, MetadataArtifact, MetadataType,
                                  RecorderType, Recording, S3VideoArtifact,
                                  SignalsArtifact, TimeWindow)
from mdfparser.interfaces.artifact_adapter import ArtifactAdapter
from mdfparser.interfaces.input_message import DataType, InputMessage


def interior_video() -> S3VideoArtifact:
    """Video artifact"""
    return S3VideoArtifact(
        artifact_id="bar_InteriorRecorder_c9e5b11d-e77a-5306-8e0b-68aa609e49db_1681370055771_1681370115771",
        raw_s3_path="s3://raw/foo/bar_InteriorRecorder_c9e5b11d-e77a-5306-8e0b-68aa609e49db_1681370055771_1681370115771.jpeg",
        anonymized_s3_path="s3://anonymized/foo/bar_InteriorRecorder_c9e5b11d-e77a-5306-8e0b-68aa609e49db_1681370055771_1681370115771_anonymized.jpeg",
        tenant_id="foo",
        device_id="bar",
        recorder=RecorderType.INTERIOR,
        timestamp=datetime.fromisoformat("2023-04-13T07:14:15.770982+00:00"),
        end_timestamp=datetime.fromisoformat(
            "2023-04-13T07:15:15.770982+00:00"),
        upload_timing=TimeWindow(
            start="2023-04-13T08:00:00+00:00",  # type: ignore
            end="2023-04-13T08:01:00+00:00"),  # type: ignore
        footage_id="c9e5b11d-e77a-5306-8e0b-68aa609e49db",
        rcc_s3_path=f"s3://rcc-bucket/key",
        recordings=[Recording(recording_id="TrainingRecorder-abc", chunk_ids=[1, 2, 3])]
    )


def training_video() -> S3VideoArtifact:
    """Video artifact"""
    return S3VideoArtifact(
        artifact_id="bar_TrainingRecorder_64594ea7-b817-54db-bdf3-dba7e2bfb186_1681370055771_1681370115771",
        tenant_id="foo",
        device_id="bar",
        raw_s3_path="s3://raw/foo/bar_TrainingRecorder_64594ea7-b817-54db-bdf3-dba7e2bfb186_1681370055771_1681370115771.jpeg",
        anonymized_s3_path="s3://anonymized/foo/bar_TrainingRecorder_64594ea7-b817-54db-bdf3-dba7e2bfb186_1681370055771_1681370115771_anonymized.jpeg",
        recorder=RecorderType.TRAINING,
        timestamp=datetime.fromisoformat("2023-04-13T07:14:15.770982+00:00"),
        end_timestamp=datetime.fromisoformat(
            "2023-04-13T07:15:15.770982+00:00"),
        upload_timing=TimeWindow(
            start="2023-04-13T08:00:00+00:00",  # type: ignore
            end="2023-04-13T08:01:00+00:00"),  # type: ignore
        footage_id="64594ea7-b817-54db-bdf3-dba7e2bfb186",
        rcc_s3_path=f"s3://rcc-bucket/key",
        recordings=[Recording(recording_id="TrainingRecorder-abc", chunk_ids=[1, 2, 3])]
    )


def imu_artifact() -> IMUArtifact:
    """IMU artifact"""
    return IMUArtifact(
        tenant_id="tid",
        device_id="devid",
        s3_path="s3://some_path/file.mp4",
        referred_artifact=training_video(),
        metadata_type=MetadataType.IMU)


def metadata_artifact() -> SignalsArtifact:
    """Metadata Artifact"""
    return SignalsArtifact(tenant_id="tid",
                           device_id="devid",
                           s3_path="s3://some_path/file.mp4",
                           referred_artifact=interior_video(),
                           metadata_type=MetadataType.SIGNALS)


@mark.unit
class TestMessageAdapter():
    """ Tests the Message Adapter class. """

    @mark.parametrize("artifact,input_message",
                      [(imu_artifact(),
                        InputMessage(id="bar_TrainingRecorder_64594ea7-b817-54db-bdf3-dba7e2bfb186_1681370055771_1681370115771",
                                     s3_path="s3://some_path/file.mp4",
                                     data_type=DataType.IMU,
                                     tenant="tid",
                                     device_id="devid",
                                     recorder="Training",
                                     raw_s3_path="s3://raw/foo/bar_TrainingRecorder_64594ea7-b817-54db-bdf3-dba7e2bfb186_1681370055771_1681370115771.jpeg")),
                          (metadata_artifact(),
                           InputMessage(id="bar_InteriorRecorder_c9e5b11d-e77a-5306-8e0b-68aa609e49db_1681370055771_1681370115771",
                                        s3_path="s3://some_path/file.mp4",
                                        data_type=DataType.METADATA,
                                        tenant="tid",
                                        device_id="devid",
                                        recorder="Interior",
                                        raw_s3_path="s3://raw/foo/bar_InteriorRecorder_c9e5b11d-e77a-5306-8e0b-68aa609e49db_1681370055771_1681370115771.jpeg"))])
    def test_adapt_message(
            self,
            artifact: MetadataArtifact,
            input_message: InputMessage):
        """ Tests the adapt message. """
        # GIVEN
        adapter = ArtifactAdapter()

        # WHEN
        result = adapter.adapt_message(artifact)

        # THEN
        assert result == input_message
