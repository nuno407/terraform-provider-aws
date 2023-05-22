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
    """ S3 Downloader class. """

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
        return self.convert_imu_data(binary_data)

    def __get_batch_timestamp(self, line: str) -> datetime:
        """
        Parses the line header of one batch of the IMU data.

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

        return datetime.fromtimestamp(int(tokens[1]) / 1000, tz=pytz.utc)

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

    def convert_imu_data(self, data_bytes: bytes) -> pd.DataFrame:
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

        # Data to be returned
        result_dfs: list[pd.DataFrame] = []

        i: int = 0
        try:
            while i < len(lines):
                # Set Header
                utc_timestamp = self.__get_batch_timestamp(lines[i])
                i += 1

                batch_data: dict[str, Any] = {}

                # Load the imu data
                for header in IMU_BATCH_FIELDS:
                    batch_data[header] = self.__get_batch_data(lines[i], header)
                    i += 1

                # Number of samples in this chunk
                num_samples = len(list(batch_data.values())[0])

                # Create a series of timestamps separated by DELTA
                # Waiting on feedback from goaldiggers to align the right way to calculate this
                utc_timestamps = utc_timestamp + \
                    pd.to_timedelta(
                        np.arange(
                            0,
                            num_samples) *
                        IMU_SAMPLE_DELTA,
                        unit="us")  # type: ignore
                batch_data["timestamp"] = utc_timestamps
                result_dfs.append(pd.DataFrame(batch_data))

        except FailToParseIMU as excpt:
            raise FailToParseIMU(f"{str(excpt)} at line: {i+1}") from excpt
        except Exception as excpt:
            raise FailToParseIMU(f"Failed to parse IMU data at line: {i+1}") from excpt

        return pd.concat(result_dfs)
