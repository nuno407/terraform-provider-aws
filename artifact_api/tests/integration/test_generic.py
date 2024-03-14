# pylint: disable=too-many-arguments, unused-argument, too-few-public-methods
""" Integration test. """
import os
from typing import Optional, Union
import pytest
from motor.motor_asyncio import AsyncIOMotorClient
from base.testing.utils import load_relative_json_file
from helper_functions import assert_mongo_state, assert_voxel_state
from httpx import AsyncClient
from freezegun import freeze_time


def get_json_message(file_name: str) -> dict:
    """Returns and parses a json file to string format"""
    return load_relative_json_file(__file__, os.path.join("test_data", file_name))


class TestGeneric:
    """
    Class that tests the entire component end2end
    """
    @ pytest.mark.integration
    @ pytest.mark.parametrize("input_json_message_list, endpoints, voxel_config, \
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
             get_json_message("sos_operator_api_message.json"),
             get_json_message("other_operator_api_message.json")],
            "/ridecare/operator",
            "voxel_config.yml",
            "mongo_config.yml",
            "mongo_operator_state.json",
            None,
        ),

        # Tests snapshot metadata
        (
            [get_json_message("training_snapshot_api_metadata_message.json"),
             get_json_message("training_snapshot_api_metadata_empty_message.json")],
            "/ridecare/signals/snapshot",
            "voxel_config.yml",
            "mongo_config.yml",
            None,
            "voxel_snapshot_metadata_state.json",
        ),

        # Tests pipeline processing status
        (
            [get_json_message("sdm_pipeline_status_api_message.json")],
            "/ridecare/pipeline/status",
            "voxel_config.yml",
            "mongo_config.yml",
            "mongo_sdm_status_state.json",
            "voxel_sdm_status_state.json",
        ),
        # Test metadata from video signals after video
        (
            [
                get_json_message("video_api_message.json"),
                get_json_message("video_signals_data_api_message.json")
            ],
            [
                "/ridecare/video",
                "/ridecare/signals/video"
            ],
            "voxel_config.yml",
            "mongo_config.yml",
            "mongo_video_signals_state.json",
            "voxel_video_signals_state.json",
        ),
        # Test metadata from video signals before video
        (
            [
                get_json_message("video_signals_data_api_message.json"),
                get_json_message("video_api_message.json"),
            ],
            [
                "/ridecare/signals/video",
                "/ridecare/video"
            ],
            "voxel_config.yml",
            "mongo_config.yml",
            "mongo_video_signals_state.json",
            "voxel_video_signals_state.json",
        ),
        # Test anonymize result video after sdm pipeline
        (
            [
                get_json_message("sdm_pipeline_status_api_message.json"),
                get_json_message("anon_result_api_video_message.json")
            ],
            [
                "ridecare/pipeline/status",
                "ridecare/pipeline/anonymize/video"
            ],
            "voxel_config.yml",
            "mongo_config.yml",
            "mongo_anon_video_state.json",
            "voxel_anon_video_state.json",
        ),
        # Test anonymize result snapshot after sdm pipeline
        (
            [
                get_json_message(
                    "sdm_pipeline_snapshot_status_api_message.json"),
                get_json_message("anon_result_api_snapshot_message.json")
            ],
            [
                "ridecare/pipeline/status",
                "ridecare/pipeline/anonymize/snapshot"
            ],
            "voxel_config.yml",
            "mongo_config.yml",
            "mongo_anon_snapshot_state.json",
            "voxel_anon_snapshot_state.json"
        ),
        # Test chc pipeline status after sdm pipeline
        (
            [
                get_json_message("sdm_chc_api_message.json"),
                get_json_message("chc_result_api_video_message.json")
            ],
            [
                "ridecare/pipeline/status",
                "ridecare/pipeline/chc/video"
            ],
            "voxel_config.yml",
            "mongo_config.yml",
            "mongo_chc_video_state.json",
            "voxel_chc_video_state.json"

        )
    ],
        ids=[
        "test_snap_artifact",
        "test_video_artifact",
        "test_events_artifact",
        "test_sav_artifact",
        "test_snap_metadata",
        "test_sdm_status",
        "test_video_signals_after_video",
        "test_video_signals_before_video",
        "test_anon_result_video",
        "test_anon_result_snapshot",
        "test_chc_result_video",
    ],
        indirect=["mongo_api_config", "voxel_config"])
    @freeze_time("2030-01-14")
    @pytest.mark.asyncio
    async def test_component(self,
                             input_json_message_list: list[dict],
                             endpoints: Union[str, list[str]],
                             voxel_config: str,
                             mongo_api_config: str,
                             expected_mongo_state: Optional[str],
                             expected_voxel_state: Optional[str],
                             api_client: AsyncClient,
                             mongo_client: AsyncIOMotorClient,
                             debug: bool = False
                             ):
        """
        This test will test the entire component.

        REMARKS:
        The S3 is not mocked, thus this WILL NOT test that the compute_metadata function is called correctly!

        To debug the tests, set the debug flag to True and run one test at a time,
        this will dump the state of the mongo and voxel database into seperated files in the same
        path where the tests were run. (mongo_dump.json and voxel_dump.json)
        The test will always fail in debug mode to avoid accidental pushes but
        will allow to check the state of the database after the test has finished.

        Args:
            input_sqs_message_list (list[str]): _description_
            output_sqs_message (str): _description_
            input_sqs_controller (SQSController): _description_
            metadata_sqs_controller (SQSController): _description_
            main_function (Callable): _description_
        """
        if isinstance(endpoints, str):
            endpoints = [endpoints for _ in input_json_message_list]

        # WHEN
        for input_json_message, endpoint in zip(input_json_message_list, endpoints):
            response = await api_client.post(endpoint, json=input_json_message)
            assert response.status_code == 200

        # THEN
        if expected_mongo_state is not None:
            await assert_mongo_state(expected_mongo_state, mongo_client, debug)
        if expected_voxel_state is not None:
            assert_voxel_state(expected_voxel_state, debug)

        assert debug is False  # Avoids accidental pushes in debug mode
