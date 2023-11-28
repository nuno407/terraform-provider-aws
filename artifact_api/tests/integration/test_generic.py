# pylint: disable=too-many-arguments, unused-argument, too-few-public-methods
""" Integration test. """
import os
from typing import Optional
import pytest
from mongomock import MongoClient as MongoMockClient
from fastapi.testclient import TestClient
from base.testing.utils import load_relative_json_file
from helper_functions import assert_mongo_state, assert_voxel_state


def get_json_message(file_name: str) -> dict:
    """Returns and parses a json file to string format"""
    return load_relative_json_file(__file__, os.path.join("test_data", file_name))


class TestGeneric:
    """
    Class that tests the entire component end2end
    """
    @ pytest.mark.integration
    @ pytest.mark.parametrize("input_json_message_list, endpoint, voxel_config, \
                              mongo_api_config, expected_mongo_state, expected_voxel_state", [
        # Test snapshot
        (
            [get_json_message("snapshot_api_message.json")],
            "/ridecare/snapshots",
            "voxel_config.yml",
            "mongo_config.yml",
            "mongo_snapshot_state.json",
            "voxel_snapshot_state.json",
        ),

        # Test video
        (
            [get_json_message("video_api_message.json")],
            "/ridecare/video",
            "voxel_config.yml",
            "mongo_config.yml",
            "mongo_video_state.json",
            "voxel_video_state.json",
        ),

        # Test events
        (
            [get_json_message("device_info_event_api_message.json"),
             get_json_message("device_incident_event_api_message.json")],
            "/ridecare/event",
            "voxel_config.yml",
            "mongo_config.yml",
            "mongo_events_state.json",
            None,
        ),

        # Tests SAV operator events
        (
            [get_json_message("camera_blocked_operator_api_message.json"),
             get_json_message("people_count_operator_api_message.json"),
             get_json_message("sos_operator_api_message.json")],
            "/ridecare/operator",
            "voxel_config.yml",
            "mongo_config.yml",
            "mongo_operator_state.json",
            None,
        )

    ],
        ids=["test_snapshot_artifact", "test_video_artifact", "test_events_artifact", "test_sav_artifact"],
        indirect=["mongo_api_config", "voxel_config"])
    def test_component(self,
                       input_json_message_list: list[dict],
                       endpoint: str,
                       voxel_config: str,
                       mongo_api_config: str,
                       expected_mongo_state: Optional[str],
                       expected_voxel_state: Optional[str],
                       api_client: TestClient,
                       mongo_client: MongoMockClient
                       ):
        """
        This test will test the entire component.

        REMARKS:
        The S3 is not mocked, thus this WILL NOT test that the compute_metadata function is called correctly!

        Args:
            input_sqs_message_list (list[str]): _description_
            output_sqs_message (str): _description_
            input_sqs_controller (SQSController): _description_
            metadata_sqs_controller (SQSController): _description_
            main_function (Callable): _description_
        """

        # WHEN
        for input_json_message in input_json_message_list:
            response = api_client.post(endpoint, json=input_json_message)
            assert response.status_code == 200

        # THEN
        if expected_mongo_state is not None:
            assert_mongo_state(expected_mongo_state, mongo_client)
        if expected_voxel_state is not None:
            assert_voxel_state(expected_voxel_state)
