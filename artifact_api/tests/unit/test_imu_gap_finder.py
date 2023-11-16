"""Tests for IMUGapFinder """

from datetime import datetime, timezone
from base.testing.utils import load_relative_json_file
import pytest
import pytz
from artifact_api.utils.imu_gap_finder import IMUGapFinder, TimeRange


def helper_load_imu_file(filename: str) -> list[dict]:
    """Helper to load imu data from a file into a list of dicts

    Args:
        filename (str): name of imu data file

    Returns:
        list[dict]: list of imu dicts, where each dict is an imu record
    """
    return load_relative_json_file(__file__, f"test_data/imu/{filename}")


@pytest.mark.unit
@pytest.mark.parametrize("file_data,list_timestamps", [
    (
        [
            {"timestamp": 1692000444123},
            {"timestamp": 1692000444523}
        ],
        [
            TimeRange(datetime.fromtimestamp(1692000444.123, timezone.utc),
                      datetime.fromtimestamp(1692000444.523, timezone.utc))
        ]
    ),
    (

        helper_load_imu_file("imu_with_gap.json"),
        [
            TimeRange(datetime(2023, 8, 17, 11, 8, 1, 850000, tzinfo=pytz.UTC),
                      datetime(2023, 8, 17, 11, 8, 13, 450000, tzinfo=pytz.UTC)),
            TimeRange(datetime(2023, 8, 17, 11, 8, 51, 190000, tzinfo=pytz.UTC),
                      datetime(2023, 8, 17, 11, 9, 0, 520000, tzinfo=pytz.UTC))
        ]
    ),
    (

        helper_load_imu_file("imu_no_gap.json"),
        [
            TimeRange(datetime(2023, 8, 17, 11, 8, 1, 850000, tzinfo=pytz.UTC),
                      datetime(2023, 8, 17, 11, 8, 2, 100000, tzinfo=pytz.UTC))
        ]
    )

], ids=["validate_timerange_test_1", "validate_timerange_test_2", "validate_timerange_test_3"])
def test_get_valid_imu_time_ranges(
        file_data: list[dict],
        list_timestamps: list[TimeRange],
        imu_gap_finder: IMUGapFinder):
    """Test for get_valid_imu_time_ranges method

    Args:
        file_data (list[dict]): list of imu dicts, where each dict is an imu record
        list_timestamps (list[TimeRange]): List of expected timestamps
        imu_gap_finder (IMUGapFinder): class where the tested method is defined
    """
    result_timeranges = imu_gap_finder.get_valid_imu_time_ranges(file_data)
    assert list_timestamps == result_timeranges
