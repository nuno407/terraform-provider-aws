import os
import pytest
import json
import pytz
from pydantic import BaseModel
from base.testing.utils import load_relative_json_file
from base.model.artifacts import OtherOperatorArtifact, PeopleCountOperatorArtifact, OperatorAdditionalInformation, Artifact, parse_all_models, VideoUploadRule, SnapshotUploadRule, DEFAULT_RULE, SelectorRule, RuleOrigin
from datetime import datetime


def load_sqs_json(fixture_file_id: str) -> dict[str, str]:
    return load_relative_json_file(__file__, os.path.join(
        "artifacts", fixture_file_id))


datetime_format = "%Y-%m-%dT%H:%M:%S.%f%z"


@pytest.mark.unit
@pytest.mark.parametrize("input_message,expected", [
    (
        load_sqs_json("sav-people_count_artifact.json"),
        PeopleCountOperatorArtifact(
            tenant_id="deepsensation",
            device_id="ivs_slimscaley_develop_bic2hi_01",
            event_timestamp=datetime.strptime("2023-09-13T11:10:25.746000+00:00", datetime_format),
            operator_monitoring_start=datetime.strptime(
                "2023-09-13T11:11:10.585000+00:00", datetime_format),
            operator_monitoring_end=datetime.strptime(
                "2023-09-13T11:13:02.256000+00:00", datetime_format),
            additional_information=OperatorAdditionalInformation(
                is_door_blocked=True,
                is_camera_blocked=False,
                is_audio_malfunction=True,
                observations="Garfield the cat blocked the door"
            ),
            artifact_name="sav-operator-people-count",
            is_people_count_correct=True
        )
    ),
    (
        load_sqs_json("sav-people_count_artifact_no_observations.json"),
        PeopleCountOperatorArtifact(
            tenant_id="deepsensation",
            device_id="ivs_slimscaley_develop_bic2hi_01",
            event_timestamp=datetime.strptime("2023-09-13T11:10:25.746000+00:00", datetime_format),
            operator_monitoring_start=datetime.strptime(
                "2023-09-13T11:11:10.585000+00:00", datetime_format),
            operator_monitoring_end=datetime.strptime(
                "2023-09-13T11:13:02.256000+00:00", datetime_format),
            additional_information=OperatorAdditionalInformation(
                is_door_blocked=True,
                is_camera_blocked=False,
                is_audio_malfunction=True,
                observations=None
            ),
            artifact_name="sav-operator-people-count",
            is_people_count_correct=True
        )
    ),
    (
        load_sqs_json("video_upload_message.json"),
        VideoUploadRule(
            tenant="datanauts",
            raw_file_path="s3://dev-rcd-raw-video-files/datanauts/some_id.mp4",
            rule=DEFAULT_RULE,
            video_id="some_id",
            footage_from=datetime(year=2023, month=5, day=10, hour=1, tzinfo=pytz.UTC),
            footage_to=datetime(year=2023, month=5, day=10, hour=1, minute=10, tzinfo=pytz.UTC),
        )
    ),
    (
        load_sqs_json("snapshot_upload_message.json"),
        SnapshotUploadRule(
            tenant="datanauts",
            raw_file_path="s3://dev-rcd-raw-video-files/datanauts/some_id.jpeg",
            rule=SelectorRule(rule_name="some_name", rule_version="1.0", origin=RuleOrigin.INTERIOR),
            snapshot_id="some_id",
            snapshot_timestamp=datetime(year=2023, month=5, day=10, hour=1, tzinfo=pytz.UTC)
        )
    ),
    (
        load_sqs_json("sav_other_artifact.json"),
        OtherOperatorArtifact(
            tenant_id="deepsensation",
            device_id="ivs_slimscaley_develop_bic2hi_01",
            event_timestamp=datetime.strptime("2023-09-13T11:10:25.746000+00:00", datetime_format),
            operator_monitoring_start=datetime.strptime(
                "2023-09-13T11:11:10.585000+00:00", datetime_format),
            operator_monitoring_end=datetime.strptime(
                "2023-09-13T11:13:02.256000+00:00", datetime_format),
            additional_information=OperatorAdditionalInformation(
                is_door_blocked=True,
                is_camera_blocked=False,
                is_audio_malfunction=True,
                observations=None
            ),
            artifact_name="sav-operator-other",
            type="something"  # type: ignore
        )
    )
], ids=["people_count_artifact", "people_count_artifact_no_obs", "video_upload_rule", "snapshot_upload_rule", "sav_other_artifact"])
def test_save_people_count(input_message: dict, expected: Artifact):
    assert parse_all_models(input_message) == expected
    assert parse_all_models(json.dumps(input_message)) == expected
