"""IMU Gap Finder"""
from collections import namedtuple
from datetime import timedelta
from kink import inject
import pandas as pd

TimeRange = namedtuple("TimeRange", ["min", "max"])


@inject
class IMUGapFinder:  # pylint:disable=too-few-public-methods
    """IMU Gap Finder Class"""

    def __init__(self):
        self.__local_counter = 0  # Used to find the gaps in IMU

    def __concat_window(self, diff) -> int:
        """
        Used on pandas apply function to find gaps in the imu data

        Args:
            diff (int): The diff column

        Returns:
            int: The index of a continues IMU
        """
        tmp_idx = self.__local_counter
        if diff > timedelta(seconds=1):
            self.__local_counter += 1

        return tmp_idx

    def get_valid_imu_time_ranges(self, imu_data: list[dict]) -> list[TimeRange]:
        """
        Return time ranges where IMU is valid.
        If only one timerange was returned, all IMU data is valid.

        Args:
            imu_data (list[dict]): The IMU data in json format.

        Returns:
            list[TimeRange]: A list of time ranges.
        """
        self.__local_counter = 0

        data_frame = pd.DataFrame([{"timestamp": row["timestamp"]} for row in imu_data])

        data_frame["timestamp"] = pd.to_datetime(data_frame["timestamp"], unit="ms", utc=True)
        data_frame["diff"] = data_frame["timestamp"].diff().fillna(timedelta(microseconds=1))
        data_frame = data_frame[["timestamp", "diff"]]
        data_frame["index_windows"] = data_frame["diff"].apply(self.__concat_window)
        data_frame = data_frame[data_frame["diff"] < timedelta(seconds=1)]

        result_data_frame = data_frame.groupby("index_windows")["timestamp"].agg(["min", "max"])
        timestamps_list = [
            TimeRange(dt_min.to_pydatetime(),
                      dt_max.to_pydatetime()) for dt_min, dt_max in result_data_frame.itertuples(index=False, name=None)
        ]
        return timestamps_list