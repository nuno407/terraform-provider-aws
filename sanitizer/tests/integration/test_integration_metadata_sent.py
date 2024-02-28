""" Integration test. """
from base.testing.utils import load_relative_str_file
from unittest.mock import Mock
import json
from typing import Callable
import os
import pytest
from base.aws.sqs import SQSController


def get_sqs_message(file_name: str) -> str:
    return load_relative_str_file(__file__, os.path.join("data", file_name))


class TestMessageSentMetadata:

    @ pytest.mark.integration
    @ pytest.mark.parametrize("input_sqs_message, output_sqs_message",
                              [(get_sqs_message("people_count_operator_input.json"),
                                get_sqs_message("people_count_operator_output.json"),
                                ),
                                  (get_sqs_message("camera_blocked_operator_input.json"),
                                   get_sqs_message("camera_blocked_operator_output.json"),
                                   ),
                                  (get_sqs_message("sos_operator_input.json"),
                                   get_sqs_message("sos_operator_output.json"),
                                   ),
                                  (get_sqs_message("other_operator_input.json"),
                                   get_sqs_message("other_operator_output.json"),
                                   )],
                              ids=["people_count_operator_artifact",
                                   "camera_blocked_operator",
                                   "sos_operator_artifact",
                                   "other_operator_artifact"])
    def test_message_sent_to_metadata(self,
                                      input_sqs_message: str,
                                      output_sqs_message: str,
                                      metadata_sqs_controller: SQSController,
                                      input_sqs_controller: SQSController,
                                      main_function: Callable):
        """
        This test function mocks the SQS and S3 and tests the component end2end.

        Args:
            input_sqs_message (str): _description_
            output_sqs_message (str): _description_
            input_sqs_controller (SQSController): _description_
            metadata_sqs_controller (SQSController): _description_
            main_function (Callable): _description_
        """
        # GIVEN
        input_sqs_controller.send_message(input_sqs_message)

        # WHEN
        main_function()

        message_body = metadata_sqs_controller.get_message(1)["Body"]
        parsed_message = json.loads(message_body)

        # THEN
        assert parsed_message == json.loads(output_sqs_message)
        assert input_sqs_controller.get_message(0) is None

    @ pytest.mark.integration
    @ pytest.mark.parametrize("input_sqs_message", [
        # Test timestamp in future
        (
            get_sqs_message("device_info_event_future_input.json")
        )

    ], ids=["device_info_event_input"])
    def test_message_metadata_error(self,
                                    input_sqs_message: str,
                                    metadata_sqs_controller: SQSController,
                                    input_sqs_controller: SQSController,
                                    main_function: Callable):
        """
        This test function mocks the SQS and S3 and tests the component end2end.

        Args:
            input_sqs_message (str): _description_
            exception_type (Exception): _description_
            metadata_sqs_controller (SQSController): _description_
            input_sqs_controller (SQSController): _description_
            main_function (Callable): _description_
        """
        # GIVEN
        input_sqs_controller.send_message(input_sqs_message)
        metadata_sqs_controller.send_message = Mock()
        input_sqs_controller.delete_message = Mock()

        # WHEN
        main_function()

        metadata_sqs_controller.send_message.assert_not_called()
        input_sqs_controller.delete_message.assert_not_called()
        assert metadata_sqs_controller.get_message(0) is None
