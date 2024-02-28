import json
from datetime import datetime, timedelta

from pytest import mark, raises
from pytz import UTC

from base.model.artifacts import (Artifact, MultiSnapshotArtifact,
                                  PreviewSignalsArtifact, RecorderType,
                                  S3VideoArtifact, SnapshotArtifact,
                                  TimeWindow, parse_artifact, Recording, OperatorSOSReason,
                                  CameraBlockedOperatorArtifact, PeopleCountOperatorArtifact,
                                  SOSOperatorArtifact, OperatorAdditionalInformation, OtherOperatorArtifact)
from base.timestamps import from_epoch_seconds_or_milliseconds


def static_date() -> datetime:
    return datetime(year=2022, month=10, day=10, hour=2, minute=10, tzinfo=UTC)


def json_snapshot() -> str:
    return """
    {
        "tenant_id": "foo",
        "artifact_id": "foo_bar_abc_1681370055771",
        "raw_s3_path": "s3://raw/foo/foo_bar_abc_1681370055771.jpeg",
        "anonymized_s3_path": "s3://anonymized/foo/foo_bar_abc_1681370055771_anonymized.jpeg",
        "artifact_name":"snapshot",
        "device_id": "bar",
        "recorder": "TrainingMultiSnapshot",
        "timestamp": "2023-04-13T07:14:15.770982Z",
        "end_timestamp" : "2023-04-13T07:14:15.770982Z",
        "upload_timing": {
            "start": "2023-04-13T08:00:00+00:00",
            "end": "2023-04-13T08:01:00+00:00"
        },
        "uuid": "abc"
    }
    """


def snapshot() -> SnapshotArtifact:
    return SnapshotArtifact(
        artifact_id="foo_bar_abc_1681370055771",
        tenant_id="foo",
        device_id="bar",
        raw_s3_path="s3://raw/foo/foo_bar_abc_1681370055771.jpeg",
        anonymized_s3_path="s3://anonymized/foo/foo_bar_abc_1681370055771_anonymized.jpeg",
        recorder=RecorderType.SNAPSHOT,
        timestamp=datetime.fromisoformat("2023-04-13T07:14:15.770982+00:00"),
        end_timestamp=datetime.fromisoformat("2023-04-13T07:14:15.770982+00:00"),
        upload_timing=TimeWindow(
            start="2023-04-13T08:00:00+00:00",  # type: ignore
            end="2023-04-13T08:01:00+00:00"),  # type: ignore
        uuid="abc"
    )


def json_video() -> str:
    return """
    {
        "tenant_id": "foo",
        "artifact_name":"s3_video",
        "artifact_id": "bar_InteriorRecorder_my_footage_id_1681370055774_1681370115774",
        "raw_s3_path": "s3://raw/foo/bar_InteriorRecorder_my_footage_id_1681370055774_1681370115774.mp4",
        "anonymized_s3_path": "s3://anonymized/foo/bar_InteriorRecorder_my_footage_id_1681370055774_1681370115774_anonymized.mp4",
        "device_id": "bar",
        "recorder": "InteriorRecorder",
        "timestamp": "2023-04-13T07:14:15.774082Z",
        "end_timestamp": "2023-04-13T07:15:15.774082Z",
        "upload_timing": {
            "start": "2023-04-13T08:00:00+00:00",
            "end": "2023-04-13T08:01:00+00:00"
        },
        "footage_id": "my_footage_id",
        "rcc_s3_path": "s3://bucket/key",
        "recordings": [{
            "chunk_ids":[1,2,3],
            "recording_id": "TrainingRecorder-abc"
        }]
    }
    """


def video() -> S3VideoArtifact:
    return S3VideoArtifact(
        artifact_id="bar_InteriorRecorder_my_footage_id_1681370055774_1681370115774",
        raw_s3_path="s3://raw/foo/bar_InteriorRecorder_my_footage_id_1681370055774_1681370115774.mp4",
        anonymized_s3_path="s3://anonymized/foo/bar_InteriorRecorder_my_footage_id_1681370055774_1681370115774_anonymized.mp4",
        tenant_id="foo",
        device_id="bar",
        recorder=RecorderType.INTERIOR,
        timestamp=datetime.fromisoformat("2023-04-13T07:14:15.774082+00:00"),
        end_timestamp=datetime.fromisoformat("2023-04-13T07:15:15.774082+00:00"),
        upload_timing=TimeWindow(
            start=datetime.fromisoformat("2023-04-13T08:00:00+00:00"),
            end=datetime.fromisoformat("2023-04-13T08:01:00+00:00")),
        footage_id="my_footage_id",
        rcc_s3_path="s3://bucket/key",
        recordings=[
            Recording(
                recording_id="TrainingRecorder-abc",
                chunk_ids=[
                    1,
                    2,
                    3])])


def multi_snapshot() -> MultiSnapshotArtifact:
    return MultiSnapshotArtifact(
        tenant_id="ridecare_companion_fut",
        artifact_id="ridecare_companion_fut_rc_srx_prod_86540229e4d69c93a329000bfc8dc6b120272cbc_InteriorRecorderPreview-145c7e01-5278-4f2b-8637-40f3f027a4b8_1685544513752",
        device_id="rc_srx_prod_86540229e4d69c93a329000bfc8dc6b120272cbc",
        timestamp=from_epoch_seconds_or_milliseconds(1685544513752),
        end_timestamp=from_epoch_seconds_or_milliseconds(1685544573758),
        recording_id="InteriorRecorderPreview-145c7e01-5278-4f2b-8637-40f3f027a4b8",
        upload_timing=TimeWindow(
            start=datetime.fromisoformat("2023-05-31T14:03:51.613360+00:00"),
            end=datetime.fromisoformat("2023-05-31T15:03:51.613360+00:00")),
        recorder=RecorderType.INTERIOR_PREVIEW,
        chunks=[
            SnapshotArtifact(
                uuid="InteriorRecorderPreview_InteriorRecorderPreview-145c7e01-5278-4f2b-8637-40f3f027a4b8_61.jpeg",
                artifact_id="test3",
                device_id="rc_srx_prod_86540229e4d69c93a329000bfc8dc6b120272cbc",
                tenant_id="ridecare_companion_fut",
                raw_s3_path="s3://raw/ridecare_companion_fut/InteriorRecorderPreview_InteriorRecorderPreview-145c7e01-5278-4f2b-8637-40f3f027a4b8_61.jpeg",
                anonymized_s3_path="s3://anonymized/ridecare_companion_fut/InteriorRecorderPreview_InteriorRecorderPreview-145c7e01-5278-4f2b-8637-40f3f027a4b8_61.jpeg",
                timestamp=from_epoch_seconds_or_milliseconds(1685544513752),
                end_timestamp=from_epoch_seconds_or_milliseconds(1685544543757),
                recorder=RecorderType.INTERIOR_PREVIEW,
                upload_timing=TimeWindow(
                    start=datetime.fromisoformat("2023-05-31T14:03:51.613360+00:00"),
                    end=datetime.fromisoformat("2023-05-31T15:03:51.613360+00:00"))),
            SnapshotArtifact(
                uuid="InteriorRecorderPreview_InteriorRecorderPreview-145c7e01-5278-4f2b-8637-40f3f027a4b8_62.jpeg",
                artifact_id="test4",
                device_id="rc_srx_prod_86540229e4d69c93a329000bfc8dc6b120272cbc",
                tenant_id="ridecare_companion_fut",
                raw_s3_path="s3://raw/ridecare_companion_fut/InteriorRecorderPreview_InteriorRecorderPreview-145c7e01-5278-4f2b-8637-40f3f027a4b8_62.jpeg",
                anonymized_s3_path="s3://anonymized/ridecare_companion_fut/InteriorRecorderPreview_InteriorRecorderPreview-145c7e01-5278-4f2b-8637-40f3f027a4b8_62.jpeg",
                timestamp=from_epoch_seconds_or_milliseconds(1685544543757),
                end_timestamp=from_epoch_seconds_or_milliseconds(1685544573758),
                recorder=RecorderType.INTERIOR_PREVIEW,
                upload_timing=TimeWindow(
                    start=datetime.fromisoformat("2023-05-31T14:03:51.613360+00:00"),
                    end=datetime.fromisoformat("2023-05-31T15:03:51.613360+00:00")))])


def preview_signals() -> PreviewSignalsArtifact:
    return PreviewSignalsArtifact(
        tenant_id="ridecare_companion_fut",
        device_id="rc_srx_prod_86540229e4d69c93a329000bfc8dc6b120272cbc",
        timestamp=static_date() - timedelta(minutes=2),
        end_timestamp=static_date() - timedelta(minutes=1),
        referred_artifact=multi_snapshot()
    )


def operator_additional_information() -> OperatorAdditionalInformation:
    return OperatorAdditionalInformation(
        is_door_blocked=True,
        is_camera_blocked=True,
        is_audio_malfunction=True,
        observations="mocked",
    )


def camera_blocked_operator() -> CameraBlockedOperatorArtifact:
    return CameraBlockedOperatorArtifact(
        tenant_id="ridecare_companion_fut",
        device_id="rc_srx_prod_86540229e4d69c93a329000bfc8dc6b120272cbc",
        operator_monitoring_start=static_date() - timedelta(minutes=2),
        operator_monitoring_end=static_date() - timedelta(minutes=1),
        event_timestamp=static_date() - timedelta(minutes=2),
        additional_information=operator_additional_information(),
        is_chc_correct=True
    )


def people_count_operator() -> PeopleCountOperatorArtifact:
    return PeopleCountOperatorArtifact(
        tenant_id="ridecare_companion_fut",
        device_id="rc_srx_prod_86540229e4d69c93a329000bfc8dc6b120272cbc",
        operator_monitoring_start=static_date() - timedelta(minutes=2),
        operator_monitoring_end=static_date() - timedelta(minutes=1),
        event_timestamp=static_date() - timedelta(minutes=2),
        additional_information=operator_additional_information(),
        correct_count=0,
        is_people_count_correct=True,
    )


def sos_count_operator() -> SOSOperatorArtifact:
    return SOSOperatorArtifact(
        tenant_id="ridecare_companion_fut",
        device_id="rc_srx_prod_86540229e4d69c93a329000bfc8dc6b120272cbc",
        operator_monitoring_start=static_date() - timedelta(minutes=2),
        operator_monitoring_end=static_date() - timedelta(minutes=1),
        event_timestamp=static_date() - timedelta(minutes=2),
        additional_information=operator_additional_information(),
        reason=OperatorSOSReason.ACCIDENTAL
    )


def other_operator_feedback() -> OtherOperatorArtifact:
    return OtherOperatorArtifact(
        tenant_id="ridecare_companion_fut",
        device_id="rc_srx_prod_86540229e4d69c93a329000bfc8dc6b120272cbc",
        operator_monitoring_start=static_date() - timedelta(minutes=2),
        operator_monitoring_end=static_date() - timedelta(minutes=1),
        event_timestamp=static_date() - timedelta(minutes=2),
        additional_information=operator_additional_information(),
        field_type="test"
    )


@mark.unit
class TestArtifacts:
    @mark.parametrize(
        "json_artifact, expected_type, expected_artifact",
        [
            (json_video(), S3VideoArtifact, video()),
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
        json_artifact["timestamp"] = (datetime.now(tz=UTC) + timedelta(hours=1)).isoformat()
        with raises(ValueError):
            parse_artifact(json.dumps(json_artifact))

    @mark.parametrize("artifact,expected_id",
                      [[video(),
                        "bar_InteriorRecorder_my_footage_id_1681370055774_1681370115774"],
                       [snapshot(),
                        "foo_bar_abc_1681370055771"],
                       [multi_snapshot(),
                        "ridecare_companion_fut_rc_srx_prod_86540229e4d69c93a329000bfc8dc6b120272cbc_InteriorRecorderPreview-145c7e01-5278-4f2b-8637-40f3f027a4b8_1685544513752"],
                       [preview_signals(),
                        "ridecare_companion_fut_rc_srx_prod_86540229e4d69c93a329000bfc8dc6b120272cbc_InteriorRecorderPreview-145c7e01-5278-4f2b-8637-40f3f027a4b8_1685544513752_metadata_preview"],
                       [camera_blocked_operator(),
                        "sav-operator-camera-blocked_ridecare_companion_fut_rc_srx_prod_86540229e4d69c93a329000bfc8dc6b120272cbc_1665367680000"],
                       [people_count_operator(),
                        "sav-operator-people-count_ridecare_companion_fut_rc_srx_prod_86540229e4d69c93a329000bfc8dc6b120272cbc_1665367680000"],
                       [sos_count_operator(),
                        "sav-operator-sos_ridecare_companion_fut_rc_srx_prod_86540229e4d69c93a329000bfc8dc6b120272cbc_1665367680000"],
                       [other_operator_feedback(),
                        "sav-operator-other_ridecare_companion_fut_rc_srx_prod_86540229e4d69c93a329000bfc8dc6b120272cbc_1665367680000"]])
    def test_artifact_id(self, artifact: Artifact, expected_id: str):
        assert artifact.artifact_id == expected_id  # type: ignore
