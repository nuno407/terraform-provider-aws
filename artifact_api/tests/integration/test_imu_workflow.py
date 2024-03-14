# pylint: disable=too-many-arguments, unused-argument, too-few-public-methods, too-many-locals
""" Integration test. """
import os
from base64 import b64encode
import pytest
from motor.motor_asyncio import AsyncIOMotorClient
from helper_functions import assert_mongo_state
from httpx import AsyncClient
from freezegun import freeze_time
from base.testing.utils import load_relative_json_file, load_relative_raw_file


def get_json_message(file_name: str) -> dict:
    """Returns and parses a json file to string format"""
    return load_relative_json_file(__file__, os.path.join("test_data", file_name))


def get_imu_data(filename: str) -> bytes:
    """Retrieve IMU data file"""
    return load_relative_raw_file(__file__, os.path.join("test_data","parquet_data",filename))


class TestIMUWorkflow:
    """
    Class that tests the entire component end2end
    """
    @ pytest.mark.integration
    @freeze_time("2030-01-14")
    @ pytest.mark.parametrize("voxel_config,mongo_api_config",
                              [("voxel_config.yml","mongo_config.yml")], indirect=["mongo_api_config", "voxel_config"])
    @pytest.mark.asyncio
    async def test_imu(self,
                       voxel_config: str,
                       mongo_api_config: str,
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
        will allow to check the state of the database
        after the test is run.

        Args:
            voxel_config (str): The path to the voxel config file
            mongo_api_config (str): The path to the mongo config file
            api_client (AsyncClient): The client to make the requests
            mongo_client (AsyncIOMotorClient): The client to interact with the mongo database
            debug (bool, optional): Flag to enable debug mode. Defaults to False.
        """
        event_1 = get_json_message("device_incident_event_api_message.json")
        event_2 = get_json_message("device_incident_event_outside_imu_api_message.json")
        message = get_json_message("imu_api_message.json")
        endpoint = "ridecare/imu/video"
        events_endpoint = "ridecare/event"
        imu_data = get_imu_data("imu.parquet")
        expected_mongo_state = "mongo_imu_state.json"

        encoded_imu = b64encode(imu_data).decode("utf-8")
        message["data"] = encoded_imu

        # WHEN
        response_1 = await api_client.post(events_endpoint, json=event_1)
        response_2 = await api_client.post(events_endpoint, json=event_2)
        response_3 = await api_client.post(endpoint, json=message)

        # THEN
        assert response_1.status_code == 200
        assert response_2.status_code == 200
        assert response_3.status_code == 200
        await assert_mongo_state(expected_mongo_state, mongo_client, debug)
        assert debug is False  # Avoids accidental pushes in debug mode
