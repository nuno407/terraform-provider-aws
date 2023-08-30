
""" Integration test. """
from base.testing.utils import load_relative_raw_file
import json
from typing import Callable
import os
import pytest
from base.aws.sqs import SQSController


def _read_and_parse_msg_body_from_sns_topic(raw_body: str) -> dict:
    call_args = [("'", '"'), ("\n", ""), ("\\\\", ""),  # pylint: disable=invalid-string-quote
                 ("\\", ""), ('"{', "{"), ('}"', "}")]  # pylint: disable=invalid-string-quote
    for args in call_args:
        raw_body = raw_body.replace(args[0], args[1])
    return json.loads(raw_body)
# pylint: disable=missing-class-docstring,missing-function-docstring,too-few-public-methods


def get_sqs_message(file_name: str) -> str:
    return load_relative_raw_file(__file__, os.path.join("data", file_name)).decode()


class TestSanitizer:

    @ pytest.mark.integration
    @ pytest.mark.parametrize("input_sqs_message, output_sqs_message", [
        (
            get_sqs_message("people_count_operator_input.json"),
            get_sqs_message("people_count_operator_output.json"),
        ),
        (
            get_sqs_message("camera_blocked_operator_input.json"),
            get_sqs_message("camera_blocked_operator_output.json"),
        ),
        (
            get_sqs_message("sos_operator_input.json"),
            get_sqs_message("sos_operator_output.json"),
        )

    ], ids=["people_count_operator_artifact", "camera_blocked_operator", "sos_operator_artifact"])
    def test_sanitizer(self,
                       input_sqs_message: str,
                       output_sqs_message: str,
                       output_sqs_controller: SQSController,
                       input_sqs_controller: SQSController,
                       main_function: Callable):
        """
        This test function mocks the SQS and S3 and tests the component end2end.

        Args:
            input_sqs_message (str): _description_
            output_sqs_message (str): _description_
            input_sqs_controller (SQSController): _description_
            output_sqs_controller (SQSController): _description_
            main_function (Callable): _description_
        """
        # GIVEN
        input_sqs_controller.send_message(input_sqs_message)

        # WHEN
        main_function()

        message_body = output_sqs_controller.get_message(1)["Body"]
        parsed_message = _read_and_parse_msg_body_from_sns_topic(message_body)

        # THEN
        assert parsed_message["Message"]["default"] == json.loads(output_sqs_message)
        assert input_sqs_controller.get_message(0) is None
