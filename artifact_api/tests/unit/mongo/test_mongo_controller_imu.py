"""Tests for mongodb controller related to IMU ingestion"""
import math
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, call
from pytest import mark
import pandas as pd
import pytz
from base.model.artifacts.api_messages import IMUProcessedData
from base.testing.utils import get_abs_path
from artifact_api.mongo.services.mongo_event_service import MongoEventService
from artifact_api.mongo.services.mongo_imu_service import MongoIMUService
from artifact_api.models.mongo_models import DBIMUSample
from artifact_api.mongo.mongo_service import MongoService
from artifact_api.utils.imu_gap_finder import IMUGapFinder, TimeRange


def _helper_load_imu_file(filename: str) -> pd.DataFrame:
    """Helper to load imu data from a file into a list of dicts

    Args:
        filename (str): name of imu data file

    Returns:
        list[dict]: list of imu dicts, where each dict is an imu record
    """
    data = pd.read_json(get_abs_path(__file__, f"../test_data/imu/{filename}"), orient="records")
    return data


def _generate_imu_data_artifact(filename: str) -> IMUProcessedData:
    """Helper to generate IMU_DataArtifacts from given filename

    Args:
        filename (str): name of imu data file

    Returns:
        IMUDataArtifact: IMU data artifact
    """

    data = IMUProcessedData.validate(_helper_load_imu_file(filename))
    return data


@mark.unit
class TestControllerIMU:  # pylint: disable=protected-access, duplicate-code
    """Class with tests for mongodb controller related to IMU ingestion"""
    async def test_process_imu_artifact(self, mongo_controller: MongoService,  # pylint: disable=too-many-arguments
                                        mongo_imu_controller: MongoIMUService,
                                        mongo_event_controller: MongoEventService,
                                        imu_gap_finder: IMUGapFinder,
                                        imu_batch_size_10: int):
        """Test for proces_imu_artifact method

        Args:
            mongo_controller (MongoController): class where the tested method is defined
            mongo_imu_controller (MongoIMUController): the mongo imu controller
            imu_data_artifact (IMUProcessedData): imu data artifact
            time_ranges (list[TimeRange]): expected time ranges for the test
        """

        # GIVEN
        time_ranges = [
            TimeRange(datetime(2023, 8, 17, 11, 8, 1, 850000, tzinfo=pytz.UTC),
                      datetime(2023, 8, 17, 11, 8, 13, 450000, tzinfo=pytz.UTC)),
            TimeRange(datetime(2023, 8, 17, 11, 8, 51, 190000, tzinfo=pytz.UTC),
                      datetime(2023, 8, 17, 11, 9, 0, 520000, tzinfo=pytz.UTC))
        ]

        dataframe = _generate_imu_data_artifact("imu_with_gap.json")
        mongo_event_controller.update_events = AsyncMock()
        mongo_imu_controller.insert_imu_data = AsyncMock()
        imu_gap_finder.get_valid_imu_time_ranges = MagicMock(return_value=time_ranges)

        num_calls = math.ceil(len(dataframe) / imu_batch_size_10)

        # WHEN
        await mongo_controller.process_imu_artifact("datanauts",dataframe)

        # THEN
        assert mongo_imu_controller.insert_imu_data.call_count == num_calls
        mongo_event_controller.update_events.assert_has_calls([
            call(rng, "datanauts", "DATANAUTS_DEV_01", "imu") for rng in time_ranges], any_order=True)

    @mark.unit
    async def test_insert_imu_data(self, processed_imu_engine: MagicMock,
                                   mongo_imu_controller: MongoIMUService):
        """Test for insert_mdf_imu_data method

        Args:
            processed_imu_engine (MagicMock): Test for update_videos_correlations method
            imu_data_list (list[dict]): list of imu documents
            mongo_controller (MongoController): class where the tested method is defined
            time_ranges (list[TimeRange]): expected time ranges for the test
        """
        # GIVEN
        imu_input = _generate_imu_data_artifact("imu_with_gap.json")
        processed_imu_engine.save_all = AsyncMock()
        imu_data = [DBIMUSample.model_validate(row.to_dict()) for _, row in imu_input.iterrows()]

        # WHEN
        await mongo_imu_controller.insert_imu_data(imu_input)

        # THEN
        processed_imu_engine.save_all.assert_called_once_with(imu_data)

    @mark.parametrize("imu_range",
                      [
                          TimeRange(datetime(2023, 8, 17, 11, 8, 1, 850000, tzinfo=pytz.UTC),
                                    datetime(2023, 8, 17, 11, 8, 13, 450000, tzinfo=pytz.UTC)),
                          TimeRange(datetime(2023, 8, 17, 11, 8, 51, 190000, tzinfo=pytz.UTC),
                                    datetime(2023, 8, 17, 11, 9, 0, 520000, tzinfo=pytz.UTC))
                      ]
                      )
    async def test_update_events(self, event_engine: MagicMock,
                                 mongo_event_controller: MongoEventService,
                                 imu_range: TimeRange):
        """Test for update_events method

        Args:
            event_engine (MagicMock): event engine mock
            mongo_controller (MongoController): class where the tested method is defined
            imu_range (TimeRange): time range for the test
        """
        # GIVEN
        event_engine.update_many = AsyncMock()
        imu_tenant = "datanauts"
        imu_device = "DATANAUTS_DEV_01"
        data_type = "imu"
        expected_query_events = {"$and": [
            {"last_shutdown.timestamp": {"$exists": False}},
            {"tenant_id": imu_tenant},
            {"device_id": imu_device},
            {"timestamp": {"$gte": imu_range.min}},
            {"timestamp": {"$lte": imu_range.max}},
        ]}

        expected_query_shutdowns = {"$and": [
            {"last_shutdown.timestamp": {"$exists": True}},
            {"last_shutdown.timestamp": {"$ne": None}},
            {"tenant_id": imu_tenant},
            {"device_id": imu_device},
            {"last_shutdown.timestamp": {"$gte": imu_range.min}},
            {"last_shutdown.timestamp": {"$lte": imu_range.max}},
        ]}

        # WHEN
        await mongo_event_controller.update_events(imu_range, imu_tenant, imu_device, data_type)

        # THEN
        event_engine.update_many.assert_has_calls([
            call(expected_query_events,
                 {
                     "$set": {
                         data_type + "_available": True
                     }
                 }
                 ),
            call(expected_query_shutdowns,
                 {
                     "$set": {
                         "last_shutdown." + data_type + "_available": True
                     }
                 }
                 )
        ])
