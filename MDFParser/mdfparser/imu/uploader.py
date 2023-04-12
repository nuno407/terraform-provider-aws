""" This module contains the Uploader class. """
import json
import logging
from datetime import datetime
from typing import Any

import pandas as pd
from mdfparser.interfaces.s3_interaction import S3Interaction

_logger = logging.getLogger("mdfparser." + __name__)


class DateTimeEncoder(json.JSONEncoder):
    """Decoder for datetime serialization"""

    def default(self, o: object) -> Any:
        if isinstance(o, datetime):
            return int(o.timestamp() * 1000)

        return json.JSONEncoder.default(self, o)


class IMUUploader(S3Interaction):  # pylint: disable=too-few-public-methods
    """ This class contains methods to upload files to S3. """

    def upload(self, processed_imu: pd.DataFrame, bucket: str, key: str) -> None:
        """
        Uploads the process IMU data to the S3.

        Args:
            imu_data (str): The IMU data in string format.
            bucket (str): The bucket to be uploaded
            key (str): The key of the file.
        """
        # Converts the processed data into bson format
        dict_data = processed_imu.to_dict(orient="records")
        serialized_data = json.dumps(dict_data, cls=DateTimeEncoder)

        self._container_services.upload_file(self._s3_client, serialized_data, bucket, key)
