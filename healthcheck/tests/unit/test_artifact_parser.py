"""unit tests for artifact parser module."""
import os
import json
import pytest
from datetime import datetime
from healthcheck.model import SQSMessage, MessageAttributes, Artifact, VideoArtifact, SnapshotArtifact
from healthcheck.artifact_parser import ArtifactParser
from healthcheck.exceptions import InvalidMessageError

CURRENT_LOCATION = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))
TEST_DATA = os.path.join(CURRENT_LOCATION, "data")
MESSAGE_PARSER_DATA = os.path.join(TEST_DATA, "artifact_parser")


def _events_message_body(fixture_file_id: str) -> str:
    filepath = os.path.join(MESSAGE_PARSER_DATA, fixture_file_id)
    with open(filepath) as fp:
        return fp.read()


def _read_and_parse_msg_body_from_fixture(fixture_file_id: str) -> dict:
    raw_body = _events_message_body(fixture_file_id)
    call_args = [("'", '"'), ("\n", ""), ("\\\\", ""), ("\\", ""), ('"{', "{"), ('}"', "}")]
    for args in call_args:
        raw_body = raw_body.replace(args[0], args[1])
    return json.loads(raw_body)


def _valid_input_sqs_message_footage() -> SQSMessage:
    return SQSMessage(
        "foo-video-msg",
        "foo-receipt",
        "2022-12-15T16:16:32.723Z",
        _read_and_parse_msg_body_from_fixture("valid_footage_event.json"),
        MessageAttributes("datanauts", "DATANAUTS_DEV_01"))

def _valid_input_sqs_message_snapshot() -> SQSMessage:
    return SQSMessage(
        "bar-snapshot-msg",
        "bar-receipt",
        "2022-12-18T07:37:07.917Z",
        _read_and_parse_msg_body_from_fixture("valid_snapshot_event.json"),
        MessageAttributes("ridecare_companion_trial", None)
    )

def _invalid_sqs_message_message() -> SQSMessage:
    return SQSMessage(
        "bar-snapshot-msg",
        "bar-receipt",
        "2022-12-18T07:37:07.917Z",
        _read_and_parse_msg_body_from_fixture("invalid_snapshot_event_missing_chunks.json"),
        MessageAttributes("ridecare_companion_trial", None)
    )


@pytest.mark.unit
class TestArtifactParser():
    @pytest.mark.parametrize("test_case,input_message,expected,is_error",
                             [
                               ("valid_video_footage_msg",
                               _valid_input_sqs_message_footage(),
                               [VideoArtifact("datanauts",
                                              "DATANAUTS_DEV_01",
                                              "DATANAUTS_DEV_01_InteriorRecorder",
                                              datetime.fromtimestamp(1671118349783 / 1000.0),
                                              datetime.fromtimestamp(1671120149783 / 1000.0))], False),
                                ("valid_snapshot_msg",
                               _valid_input_sqs_message_snapshot(),
                               [
                                SnapshotArtifact("ridecare_companion_trial",
                                              "my_test_device",
                                              "TrainingMultiSnapshot_foo.jpeg",
                                              timestamp=datetime.fromtimestamp(1671346291000 / 1000.0)),
                                SnapshotArtifact("ridecare_companion_trial",
                                              "my_test_device",
                                              "TrainingMultiSnapshot_bar.jpeg",
                                              timestamp=datetime.fromtimestamp(1671347823000 / 1000.0))
                                ], False),
                                ("invalid_snapshot_msg_missing_chunks",
                               _invalid_sqs_message_message(),
                               [], True)
                             ])
    def test_artifact_parser(self, test_case: str, input_message: SQSMessage, expected: list[Artifact], is_error: bool):
        print("running test case", test_case)

        if is_error:
            with pytest.raises(InvalidMessageError):
                ArtifactParser().parse_message(input_message)
        else:
            got_artifacts = ArtifactParser().parse_message(input_message)
            print(got_artifacts)
            assert got_artifacts == expected
