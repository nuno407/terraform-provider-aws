"""Tests for mongodb controller related to IMU ingestion"""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, call
from pytest import mark
import pytz
from base.model.artifacts.api_messages import IMUDataArtifact, IMUSample, IMUProcessedData
from base.model.artifacts import IMUProcessingResult
from base.testing.utils import load_relative_json_file
from artifact_api.mongo.services.mongo_event_service import MongoEventService
from artifact_api.mongo.services.mongo_imu_service import MongoIMUService
from artifact_api.models.mongo_models import (DBIMUSample)
from artifact_api.mongo.mongo_service import MongoService
from artifact_api.utils.imu_gap_finder import TimeRange


def _helper_load_imu_file(filename: str) -> list[dict]:
    """Helper to load imu data from a file into a list of dicts

    Args:
        filename (str): name of imu data file

    Returns:
        list[dict]: list of imu dicts, where each dict is an imu record
    """
    return load_relative_json_file(__file__, f"test_data/imu/{filename}")


def _generate_imu_data_artifact(filename: str) -> IMUDataArtifact:
    """Helper to generate IMU_DataArtifacts from given filename

    Args:
        filename (str): name of imu data file

    Returns:
        IMUDataArtifact: IMU data artifact
    """
    message = IMUProcessingResult(
        artifact_name="imu_processed",
        s3_path=f"s3://bucket/dir/{filename}",
        correlation_id=filename.split(".")[0],
        tenant_id="datanauts",
        video_raw_s3_path="s3://bucket/dir/video.mp4"
    )

    data = IMUProcessedData.model_validate(_helper_load_imu_file(filename))
    imu_data = IMUDataArtifact(message=message, data=data)
    return imu_data


@mark.unit
class TestControllerIMU:  # pylint: disable=protected-access, duplicate-code
    """Class with tests for mongodb controller related to IMU ingestion"""
    @mark.parametrize("imu_data_artifact, time_ranges", [
        (
            _generate_imu_data_artifact("imu_with_gap.json"),
            [
                TimeRange(datetime(2023, 8, 17, 11, 8, 1, 850000, tzinfo=pytz.UTC),
                          datetime(2023, 8, 17, 11, 8, 13, 450000, tzinfo=pytz.UTC)),
                TimeRange(datetime(2023, 8, 17, 11, 8, 51, 190000, tzinfo=pytz.UTC),
                          datetime(2023, 8, 17, 11, 9, 0, 520000, tzinfo=pytz.UTC))
            ]
        ),
        (
            _generate_imu_data_artifact("imu_no_gap.json"),
            [
                TimeRange(datetime(2023, 8, 17, 11, 8, 1, 850000, tzinfo=pytz.UTC),
                          datetime(2023, 8, 17, 11, 8, 2, 100000, tzinfo=pytz.UTC))
            ]
        )
    ])
    async def test_process_imu_artifact(self, mongo_controller: MongoService,  # pylint: disable=too-many-arguments
                                        mongo_imu_controller: MongoIMUService,
                                        mongo_event_controller: MongoEventService,
                                        imu_data_artifact: IMUDataArtifact,
                                        time_ranges: list[TimeRange]):
        """Test for proces_imu_artifact method

        Args:
            mongo_controller (MongoController): class where the tested method is defined
            mongo_imu_controller (MongoIMUController): the mongo imu controller
            imu_data_artifact (IMUDataArtifact): imu data artifact
            time_ranges (list[TimeRange]): expected time ranges for the test
        """

        # GIVEN
        mongo_event_controller.update_events = AsyncMock()
        mongo_imu_controller.insert_imu_data = AsyncMock(return_value=time_ranges)

        # WHEN
        await mongo_controller.process_imu_artifact(imu_data_artifact)

        # THEN
        mongo_imu_controller.insert_imu_data.assert_called_once_with(imu_data_artifact.data.root)
        mongo_event_controller.update_events.assert_has_calls([
            call(rng, "datanauts", "DATANAUTS_DEV_01", "imu") for rng in time_ranges])

    @mark.parametrize("imu_data_list, time_ranges", [
        (
            _helper_load_imu_file("imu_with_gap.json"),
            [
                TimeRange(datetime(2023, 8, 17, 11, 8, 1, 850000, tzinfo=pytz.UTC),
                          datetime(2023, 8, 17, 11, 8, 13, 450000, tzinfo=pytz.UTC)),
                TimeRange(datetime(2023, 8, 17, 11, 8, 51, 190000, tzinfo=pytz.UTC),
                          datetime(2023, 8, 17, 11, 9, 0, 520000, tzinfo=pytz.UTC))
            ]
        ),
        (
            _helper_load_imu_file("imu_no_gap.json"),
            [
                TimeRange(datetime(2023, 8, 17, 11, 8, 1, 850000, tzinfo=pytz.UTC),
                          datetime(2023, 8, 17, 11, 8, 2, 100000, tzinfo=pytz.UTC))
            ]
        )
    ])
    async def test_insert_imu_data(self, processed_imu_engine: MagicMock,
                                   imu_data_list: list[dict],
                                   mongo_imu_controller: MongoIMUService,
                                   time_ranges: list[TimeRange]):
        """Test for insert_mdf_imu_data method

        Args:
            processed_imu_engine (MagicMock): Test for update_videos_correlations method
            imu_data_list (list[dict]): list of imu documents
            mongo_controller (MongoController): class where the tested method is defined
            time_ranges (list[TimeRange]): expected time ranges for the test
        """
        # GIVEN
        processed_imu_engine.save_all = AsyncMock()
        imu_data = [IMUSample.model_validate(item) for item in imu_data_list]
        for item in imu_data_list:
            item["timestamp"] = datetime.fromtimestamp(item["timestamp"] / 1000, tz=pytz.utc)
        expected_imu_data = [DBIMUSample.model_validate(item) for item in imu_data_list]

        # WHEN
        imu_ranges = await mongo_imu_controller.insert_imu_data(imu_data)

        # THEN
        assert imu_ranges == time_ranges
        processed_imu_engine.save_all.assert_called_once_with(expected_imu_data)

    @mark.parametrize("imu_range",
                      [
                          TimeRange(datetime(2023, 8, 17, 11, 8, 1, 850000, tzinfo=pytz.UTC),
                                    datetime(2023, 8, 17, 11, 8, 13, 450000, tzinfo=pytz.UTC)),
                          TimeRange(datetime(2023, 8, 17, 11, 8, 51, 190000, tzinfo=pytz.UTC),
                                    datetime(2023, 8, 17, 11, 9, 0, 520000, tzinfo=pytz.UTC))
                      ]
                      )
    async def test_update_events(self, event_engine: MagicMock,
                                 mongo_event_controller:
                                 MongoEventService,
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
