
""" Integration test. """
from base.testing.utils import load_relative_raw_file, assert_unordered_lists
from unittest.mock import Mock
from typing import Union
import json
from typing import Callable
import os
import pytest
from mypy_boto3_sqs import SQSClient
from base.aws.sqs import SQSController, parse_message_body_to_dict
from base.graceful_exit import GracefulExit
from unittest.mock import Mock, PropertyMock


def get_sqs_message(file_name: str) -> str:
    return load_relative_raw_file(__file__, os.path.join("data", file_name)).decode()


class TestMessagePublish:

    @ pytest.mark.integration
    @ pytest.mark.parametrize("input_sqs_messages, output_sqs_messages, config", [
        (
            [get_sqs_message("people_count_operator_input.json")],
            [get_sqs_message("people_count_operator_output.json")],
            None
        ),
        (
            [get_sqs_message("camera_blocked_operator_input.json")],
            [get_sqs_message("camera_blocked_operator_output.json")],
            None
        ),
        (
            [get_sqs_message("sos_operator_input.json")],
            [get_sqs_message("sos_operator_output.json")],
            None
        ),
        (
            [get_sqs_message("video_message_input.json")],
            [
                get_sqs_message("video_message_output.json"),
                get_sqs_message("imu_video_message_output.json"),
                get_sqs_message("metadata_video_message_output.json")
            ],
            None
        ),
        # Check that IMU is not published if it is blocked by version
        (
            [get_sqs_message("device_info_event_version_1_7_old.json"), get_sqs_message(
                "device_info_event_version_1_8.json"), get_sqs_message("video_message_input.json")],
            [
                get_sqs_message("video_message_output.json"),
                get_sqs_message("metadata_video_message_output.json")
            ],
            "device_blocked_config.yaml"
        )

    ], ids=["people_count_operator_artifact", "camera_blocked_operator", "sos_operator_artifact", "video_message", "device_filter"], indirect=["config"])
    def test_message_published(self,
                               config: dict[str, Union[str, int]],
                               input_sqs_messages: list[str],
                               output_sqs_messages: list[str],
                               output_sqs_controller: SQSController,
                               input_sqs_controller: SQSController,
                               moto_sqs_client: SQSClient,
                               gracefull_exit: GracefulExit,
                               main_function: Callable):
        """
        This test function mocks the SQS and SNS in order to test the component end2end.

        To create a new test, place the input and output message in the data folder
        and add the test to the parametrize decorator with the call to get_sqs_message.
        It's also possible to provide a config file for the test. If no config file is provided,
        a default config file is used.

        To parse a input message from the logs use the `generate_from_logs.ipynb` notebook, this will convert
        a message that is in the logs and convert it to the right format sent by the RCC.

        REMARKS:
        The output_sqs_messages have to be unique! If this is not the case,
        the assert set(parsed_message_results) == set(outparsed_messages) has to be changed

        Args:
            config (dict[str, Union[str, int]]): The config file to use for the test
            input_sqs_messages (list[str]): The input messages to send to the sanitizer
            output_sqs_messages (list[str]): The expected output messages
            output_sqs_controller (SQSController): The mocked output SQS controller
            input_sqs_controller (SQSController): The mocked input SQS controller
            moto_sqs_client (SQSClient): The mocked SQS client
            gracefull_exit (GracefulExit): The graceful exit object
            main_function (Callable): The main function of the sanitizer
        """
        # GIVEN
        for msg in input_sqs_messages:
            input_sqs_controller.send_message(msg)

        moto_sqs_client.delete_message = Mock()

        list_passes = [True for _ in range(len(input_sqs_messages))]
        list_passes.append(False)
        type(gracefull_exit).continue_running = PropertyMock(side_effect=list_passes)

        # WHEN
        main_function()

        parsed_message_results: list[dict] = []
        while msg := output_sqs_controller.get_message(0):
            parsed_body = parse_message_body_to_dict(msg["Body"], ["Message", "Body", "default"])
            parsed_message_results.append(parsed_body["Message"]["default"])

        # THEN

        # Dump parsed message results to a file (Usefull for debugging)
        # with open("parsed_message_results.json", "w") as file:
        #     json.dump(parsed_message_results, file, indent=4)

        outparsed_messages = [json.loads(output_msg) for output_msg in output_sqs_messages]

        assert_unordered_lists(parsed_message_results, outparsed_messages)
        assert input_sqs_controller.get_message(0) is None
        assert moto_sqs_client.delete_message.call_count is len(input_sqs_messages)
