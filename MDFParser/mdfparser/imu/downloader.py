""" S3 Downloader module. """
import logging
from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd
import pytz
from mdfparser.constants import (IMU_BATCH_FIELDS, IMU_BATCH_START_HEADER,
                                 IMU_DELIMITER, IMU_SAMPLE_DELTA)
from mdfparser.exceptions import FailToParseIMU
from mdfparser.interfaces.s3_interaction import S3Interaction

_logger = logging.getLogger("mdfparser." + __name__)


class IMUDownloader(S3Interaction):  # pylint: disable=too-few-public-methods
    """ S3 Downloader class.
        Repsonsible for downloading and parsing the IMU data.

        REMARKS:
        The parse of the first version of the IMU should be deprecated whenever the new IMU strcture has been
        rolled out to all devices.
    """

    def download(self, imu_path: str) -> pd.DataFrame:
        """
        Downloads and parses the IMU data into a DataFrame.

        Args:
            imu_path (str): The path to download the imu raw data.

        Returns:
            pd.DataFrame: A DataFrame with the columns specified in CHUNK_DATA + "timestamp".
        """
        bucket, key = self._get_s3_path(imu_path)
        binary_data = self._container_services.download_file(self._s3_client, bucket, key)
        return self.__convert_imu_data(binary_data)

    def __get_batch_timestamps(self, line: str) -> list[datetime]:
        """
        Parses the line header of one batch of the IMU data.
        This function just returns the start of each chunk in case of the first IMU version.
        If this is the second version of the IMU, it will return a list of timestamps for each sample.

        Args:
            line (str): The complete line of the IMU.

        Raises:
            FailToParseIMU: If the line does not start with CHUNK_HEADER

        Returns:
            datetime: The time when the batch started.
        """
        tokens = line.strip().split(IMU_DELIMITER)

        if tokens[0] != IMU_BATCH_START_HEADER:
            raise FailToParseIMU(
                f"Failed to parse IMU chunk headers, expected: {IMU_BATCH_START_HEADER} and got {tokens[0]}")

        # Handle version 2 of the IMU
        if len(tokens) > 5 and tokens[4] == "timestamps":
            start_chunk_utc_ts = int(tokens[1])
            reference_ts = int(tokens[-1])

            utc_timestamps: list[datetime] = []

            for token in tokens[5:]:
                ts_delta_ms = float((int(token) - reference_ts) / 1_000_000)
                utc_timestamp = ts_delta_ms + start_chunk_utc_ts

                utc_timestamps.append(datetime.fromtimestamp(utc_timestamp / 1000, tz=pytz.utc))
            return utc_timestamps

        return [datetime.fromtimestamp(int(tokens[1]) / 1000, tz=pytz.utc)]

    def __get_batch_data(self, line: str, expected_label: str) -> Any:
        """
        Parses a line containing multiple samples of a feature for the current batch.

        Args:
            line (str): The complete line containing the samples..
            expected_label (str): The name of the samples to be read, that is expected at the start of the line.

        Raises:
            FailToParseIMU: If the line does not start with the expected_label.

        Returns:
            np.array: A numpy array containing all the values.
        """
        tokens = line.strip().split(IMU_DELIMITER)

        if tokens[0] != expected_label:
            raise FailToParseIMU(
                f"Failed to parse IMU chunk labels, expected: {expected_label} and got: {tokens[0]}")

        return np.array(tokens[1:], dtype=np.float32)

    def __convert_imu_v1(self, csv_data: list[str]) -> list[pd.DataFrame]:
        """
        Converts the IMU data from the version 1 (Only have timestamps for the start of the chunk)
        This will be deprecated in the future.

        Args:
            json_data (list[str]): A list of lines for the raw IMU

        Returns:
            list[pd.DataFrame]: A list of DataFrames with the columns specified in CHUNK_DATA + "timestamp".
        """
        i: int = 0
        result_dfs: list[pd.DataFrame] = []
        while i < len(csv_data):
            # Set Header
            utc_timestamps = self.__get_batch_timestamps(csv_data[i])

            if len(utc_timestamps) != 1:
                raise FailToParseIMU("Unexpected number of timestamps in IMU version 1")

            utc_timestamp = utc_timestamps[0]

            i += 1
            batch_data: dict[str, Any] = {}

            # Load the imu data
            for header in IMU_BATCH_FIELDS:
                batch_data[header] = self.__get_batch_data(csv_data[i], header)
                i += 1

            # Number of samples in this chunk
            num_samples = len(list(batch_data.values())[0])

            # Create a series of timestamps separated by DELTA
            utc_timestamps = utc_timestamp + \
                pd.to_timedelta(
                    np.arange(
                        0,
                        num_samples) *
                    IMU_SAMPLE_DELTA,
                    unit="us")  # type: ignore
            batch_data["timestamp"] = utc_timestamps
            result_dfs.append(pd.DataFrame(batch_data))

        return result_dfs

    def __convert_imu_v2(self, csv_data: list[str]) -> list[pd.DataFrame]:
        """
        Converts the IMU data from the version 2 (With timestamps for each sample)

        Args:
            json_data (list[str]): A list of lines for the raw IMU

        Returns:
            list[pd.DataFrame]: A list of DataFrames with the columns specified in CHUNK_DATA + "timestamp".
        """
        # Data to be returned
        result_dfs: list[pd.DataFrame] = []

        i: int = 0
        while i < len(csv_data):
            # Set Header
            utc_timestamps = self.__get_batch_timestamps(csv_data[i])
            i += 1

            batch_data: dict[str, Any] = {}

            # Load the imu data
            for header in IMU_BATCH_FIELDS:
                batch_data[header] = self.__get_batch_data(csv_data[i], header)
                i += 1

            # Number of samples in this chunk
            num_samples = len(list(batch_data.values())[0])

            if num_samples != len(utc_timestamps):
                raise FailToParseIMU(
                    f"Number of utc timestamps {len(utc_timestamps)} is different then \
                    the number of samples {num_samples}")

            batch_data["timestamp"] = utc_timestamps
            result_dfs.append(pd.DataFrame(batch_data))
        return result_dfs

    def __convert_imu_data(self, data_bytes: bytes) -> pd.DataFrame:
        """
        Given a raw IMU csv parse and converts it to a DataFrame.
        The result dataframe will have one sample per row, and the timestamp of that sample will
        be calculated by constantly incrementing DELTA miliseconds.

        IMPORTANT REMARK:
        In order to reduce memory consumption, the data_bytes will be DELETED after the function returns.
        Thus remaining inaccessible after the call.

        Args:
            data_bytes (bytes): The raw csv file in memory.

        Raises:
            FailToParseIMU: If there was an error while parsing the IMU file.

        Returns:
            pd.DataFrame: A DataFrame with the columns specified in CHUNK_DATA + "timestamp".
        """

        # Convert byte data to string
        data: str = data_bytes.decode("utf-8")
        del data_bytes

        # Split the data into lines
        lines = data.rstrip().split("\n")
        del data

        utc_timestamps = self.__get_batch_timestamps(lines[0])

        if len(utc_timestamps) == 1:
            result_dfs = self.__convert_imu_v1(lines)
        elif len(utc_timestamps) > 1:
            result_dfs = self.__convert_imu_v2(lines)
        else:
            raise FailToParseIMU(
                f"Could't detect IMU version, number of timestamps is {len(utc_timestamps)}")

        return pd.concat(result_dfs)
