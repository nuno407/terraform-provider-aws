"""IMU Gap Finder"""
from collections import namedtuple
from datetime import timedelta
from kink import inject
from base.model.artifacts import IMUProcessedData

TimeRange = namedtuple("TimeRange", ["min", "max"])


@inject
class IMUGapFinder:  # pylint:disable=too-few-public-methods
    """IMU Gap Finder Class"""

    @staticmethod
    def get_valid_imu_time_ranges(imu_data: IMUProcessedData) -> list[TimeRange]:
        """
        Return time ranges where IMU is valid.
        If only one timerange was returned, all IMU data is valid.

        Args:
            imu_data (IMUProcessedData): The IMU data in json format.

        Returns:
            list[TimeRange]: A list of time ranges.
        """
        data_frame = imu_data[["timestamp"]].sort_values("timestamp").reset_index()
        data_frame["diff"] = data_frame["timestamp"].diff().fillna(timedelta(microseconds=1))
        data_frame["new_range"] = data_frame["diff"] > timedelta(seconds=1)

        # Gets all the indexes where the new range ends
        values = data_frame[data_frame["new_range"]].index.to_list()

        # Get's all the indexes where the new range starts
        values.extend([val - 1 for val in values])

        # Ensure we also get the first timestamp
        values.sort()

        # Replace all the indexes with the corresponding timestamps
        timestamps = data_frame.loc[values, "timestamp"].to_list()

        # Replace the last timestamp which isn't actually the end of a range
        last_timestamp = data_frame["timestamp"].max()
        start_timestamp = data_frame["timestamp"].min()

        timestamps.insert(0,start_timestamp)
        timestamps.append(last_timestamp)

        # Creates a list of time ranges
        timestamps = list(zip(timestamps,timestamps[1:]))[::2]
        return [TimeRange(min=timestamp[0].to_pydatetime(), max=timestamp[1].to_pydatetime())
                for timestamp in timestamps]
