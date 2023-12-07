""" test module for parsing snapshot artifact. """
import pytest

from base.aws.model import MessageAttributes, SQSMessage
from base.model.artifacts import (Artifact, RecorderType, SnapshotArtifact,
                                  TimeWindow)
from base.timestamps import from_epoch_seconds_or_milliseconds
from sanitizer.artifact.parsers.snapshot_parser import \
    SnapshotParser
from helper_functions import load_sqs_json
from sanitizer.config import SanitizerConfig

MESSAGE_ID = "barfoo"
RECEIPT_HANDLE = "foobar"


@pytest.mark.unit
@pytest.mark.parametrize("test_case,input_message,expected",
                         [("valid_snapshot_event",
                           SQSMessage(message_id=MESSAGE_ID,
                                      receipt_handle=RECEIPT_HANDLE,
                                      body=load_sqs_json("valid_snapshot_event.json"),
                                      timestamp="1671346291000",
                                      attributes=MessageAttributes(tenant="ridecare_companion_trial",
                                                                   device_id="rc_srx_prod_5c88ed5d1a39500838867f5fd03f8017d295250b")),
                             [SnapshotArtifact(artifact_id="ridecare_companion_trial_rc_srx_prod_5c88ed5d1a39500838867f5fd03f8017d295250b_TrainingMultiSnapshot_TrainingMultiSnapshot-a574c99e-9841-40a9-ab8b-ebbe789ae631_342_1671346291000",
                                               uuid="TrainingMultiSnapshot_TrainingMultiSnapshot-a574c99e-9841-40a9-ab8b-ebbe789ae631_342.jpeg",
                                               device_id="rc_srx_prod_5c88ed5d1a39500838867f5fd03f8017d295250b",
                                               tenant_id="ridecare_companion_trial",
                                               raw_s3_path="s3://test-raw/ridecare_companion_trial/ridecare_companion_trial_rc_srx_prod_5c88ed5d1a39500838867f5fd03f8017d295250b_TrainingMultiSnapshot_TrainingMultiSnapshot-a574c99e-9841-40a9-ab8b-ebbe789ae631_342_1671346291000.jpeg",
                                               s3_path="s3://test-raw/ridecare_companion_trial/ridecare_companion_trial_rc_srx_prod_5c88ed5d1a39500838867f5fd03f8017d295250b_TrainingMultiSnapshot_TrainingMultiSnapshot-a574c99e-9841-40a9-ab8b-ebbe789ae631_342_1671346291000.jpeg",
                                               anonymized_s3_path="s3://test-anonymized/ridecare_companion_trial/ridecare_companion_trial_rc_srx_prod_5c88ed5d1a39500838867f5fd03f8017d295250b_TrainingMultiSnapshot_TrainingMultiSnapshot-a574c99e-9841-40a9-ab8b-ebbe789ae631_342_1671346291000_anonymized.jpeg",
                                               timestamp=from_epoch_seconds_or_milliseconds(1671346291000),
                                               end_timestamp=from_epoch_seconds_or_milliseconds(1671346292000),
                                               recorder=RecorderType.SNAPSHOT,
                                               upload_timing=TimeWindow(start="2022-12-18T06:37:07.842030994Z",
                                                                        end="2022-12-18T07:37:07.842030994Z")),
                              SnapshotArtifact(artifact_id="ridecare_companion_trial_rc_srx_prod_5c88ed5d1a39500838867f5fd03f8017d295250b_TrainingMultiSnapshot_TrainingMultiSnapshot-a574c99e-9841-40a9-ab8b-ebbe789ae631_369_1671347823000",
                                               uuid="TrainingMultiSnapshot_TrainingMultiSnapshot-a574c99e-9841-40a9-ab8b-ebbe789ae631_369.jpeg",
                                               device_id="rc_srx_prod_5c88ed5d1a39500838867f5fd03f8017d295250b",
                                               raw_s3_path="s3://test-raw/ridecare_companion_trial/ridecare_companion_trial_rc_srx_prod_5c88ed5d1a39500838867f5fd03f8017d295250b_TrainingMultiSnapshot_TrainingMultiSnapshot-a574c99e-9841-40a9-ab8b-ebbe789ae631_369_1671347823000.jpeg",
                                               s3_path="s3://test-raw/ridecare_companion_trial/ridecare_companion_trial_rc_srx_prod_5c88ed5d1a39500838867f5fd03f8017d295250b_TrainingMultiSnapshot_TrainingMultiSnapshot-a574c99e-9841-40a9-ab8b-ebbe789ae631_369_1671347823000.jpeg",
                                               anonymized_s3_path="s3://test-anonymized/ridecare_companion_trial/ridecare_companion_trial_rc_srx_prod_5c88ed5d1a39500838867f5fd03f8017d295250b_TrainingMultiSnapshot_TrainingMultiSnapshot-a574c99e-9841-40a9-ab8b-ebbe789ae631_369_1671347823000_anonymized.jpeg",
                                               tenant_id="ridecare_companion_trial",
                                               timestamp=from_epoch_seconds_or_milliseconds(1671347823000),
                                               end_timestamp=from_epoch_seconds_or_milliseconds(1671347824000),
                                               recorder=RecorderType.SNAPSHOT,
                                               upload_timing=TimeWindow(start="2022-12-18T06:37:07.842030994Z",
                                                                        end="2022-12-18T07:37:07.842030994Z"))],
                           )])
def test_snapshot_parser(test_case: str,
                         input_message: SQSMessage,
                         expected: list[Artifact],
                         sanitizer_config: SanitizerConfig):
    """ Test for parsing snapshot artifact. """
    got_artifact = list(SnapshotParser(sanitizer_config=sanitizer_config).parse(input_message, RecorderType.SNAPSHOT))
    got_artifact.sort(key=lambda a: a.uuid)
    assert got_artifact == expected
