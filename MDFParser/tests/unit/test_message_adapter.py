""" Unit tests for the Downloader class. """
from datetime import datetime

from mdfparser.interfaces.artifact_adapter import ArtifactAdapter
from mdfparser.interfaces.input_message import DataType, InputMessage
from pytest import mark

from base.model.artifacts import (IMUArtifact, MetadataArtifact, MetadataType,
                                  RecorderType, TimeWindow, VideoArtifact, SignalsArtifact)


def interior_video() -> VideoArtifact:
    """Video artifact"""
    return VideoArtifact(
        tenant_id="foo",
        device_id="bar",
        recorder=RecorderType.INTERIOR,
        timestamp=datetime.fromisoformat("2023-04-13T07:14:15.770982+00:00"),
        end_timestamp=datetime.fromisoformat(
            "2023-04-13T07:15:15.770982+00:00"),
        upload_timing=TimeWindow(
            start="2023-04-13T08:00:00+00:00",  # type: ignore
            end="2023-04-13T08:01:00+00:00"),  # type: ignore
        stream_name="my_stream"
    )


def training_video() -> VideoArtifact:
    """Video artifact"""
    return VideoArtifact(
        tenant_id="foo",
        device_id="bar",
        recorder=RecorderType.TRAINING,
        timestamp=datetime.fromisoformat("2023-04-13T07:14:15.770982+00:00"),
        end_timestamp=datetime.fromisoformat(
            "2023-04-13T07:15:15.770982+00:00"),
        upload_timing=TimeWindow(
            start="2023-04-13T08:00:00+00:00",  # type: ignore
            end="2023-04-13T08:01:00+00:00"),  # type: ignore
        stream_name="my_stream"
    )


def imu_artifact() -> IMUArtifact:
    """IMU artifact"""
    return IMUArtifact("tid", "devid", "s3://some_path", training_video(), MetadataType.IMU)


def metadata_artifact() -> SignalsArtifact:
    """Metadata Artifact"""
    return SignalsArtifact("tid", "devid", "s3://some_path",
                           interior_video(), MetadataType.SIGNALS)


@mark.unit
class TestMessageAdapter():
    """ Tests the Downloader class. """

    @mark.parametrize("artifact,input_message",
                      [(imu_artifact(),
                        InputMessage(id="my_stream_1681370055771_1681370115771",
                                     s3_path="s3://some_path",
                                     data_type=DataType.IMU,
                                     tenant="tid",
                                     device_id="devid",
                                     recorder="Training")),
                          (metadata_artifact(),
                           InputMessage(id="my_stream_1681370055771_1681370115771",
                                        s3_path="s3://some_path",
                                        data_type=DataType.METADATA,
                                        tenant="tid",
                                        device_id="devid",
                                        recorder="Interior"))])
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
