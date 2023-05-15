import json
from datetime import datetime, timedelta

import pytz
from pytest import mark, raises

from base.model.artifacts import (RecorderType, SnapshotArtifact, TimeWindow,
                                  VideoArtifact, parse_artifact)


def json_snapshot() -> str:
    return """
    {
        "tenant_id": "foo",
        "device_id": "bar",
        "recorder": "TrainingMultiSnapshot",
        "timestamp": "2023-04-13T07:14:15.770982Z",
        "upload_timing": {
            "start": "2023-04-13T08:00:00+00:00",
            "end": "2023-04-13T08:01:00+00:00"
        },
        "uuid": "abc"
    }
    """


def snapshot() -> SnapshotArtifact:
    return SnapshotArtifact(
        tenant_id="foo",
        device_id="bar",
        recorder=RecorderType.SNAPSHOT,
        timestamp=datetime.fromisoformat("2023-04-13T07:14:15.770982+00:00"),
        upload_timing=TimeWindow(
            start="2023-04-13T08:00:00+00:00",  # type: ignore
            end="2023-04-13T08:01:00+00:00"),  # type: ignore
        uuid="abc"
    )


def json_video() -> str:
    return """
    {
        "tenant_id": "foo",
        "device_id": "bar",
        "recorder": "InteriorRecorder",
        "timestamp": "2023-04-13T07:14:15.770982Z",
        "end_timestamp": "2023-04-13T07:15:15.770982Z",
        "upload_timing": {
            "start": "2023-04-13T08:00:00+00:00",
            "end": "2023-04-13T08:01:00+00:00"
        },
        "stream_name": "my_stream"
    }
    """


def video() -> VideoArtifact:
    return VideoArtifact(
        tenant_id="foo",
        device_id="bar",
        recorder=RecorderType.INTERIOR,
        timestamp=datetime.fromisoformat("2023-04-13T07:14:15.770982+00:00"),
        end_timestamp=datetime.fromisoformat("2023-04-13T07:15:15.770982+00:00"),
        upload_timing=TimeWindow(
            start="2023-04-13T08:00:00+00:00",  # type: ignore
            end="2023-04-13T08:01:00+00:00"),  # type: ignore
        stream_name="my_stream"
    )


@mark.unit
class TestArtifacts:
    @mark.parametrize(
        "json_artifact, expected_type, expected_artifact",
        [
            (json_video(), VideoArtifact, video()),
            (json_snapshot(), SnapshotArtifact, snapshot())
        ]
    )
    def test_type_matches_and_is_complete(self, json_artifact, expected_type, expected_artifact):
        artifact = parse_artifact(json_artifact)
        assert isinstance(artifact, expected_type)
        assert artifact == expected_artifact

    @mark.parametrize(
        "json_artifact",
        [
            json_video(),
            json_snapshot()
        ]
    )
    def test_parse_artifact_raises_on_unknown_recorder(self, json_artifact: str):
        artifact_dict: dict = json.loads(json_artifact)
        artifact_dict["recorder"] = "Unknown"
        with raises(ValueError):
            parse_artifact(json.dumps(artifact_dict))

    def test_timestamp_newer_than_now_raises(self):
        json_artifact = json.loads(json_video())
        json_artifact["timestamp"] = (datetime.now(tz=pytz.UTC) + timedelta(hours=1)).isoformat()
        with raises(ValueError):
            parse_artifact(json.dumps(json_artifact))
