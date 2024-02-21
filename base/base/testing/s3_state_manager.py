"""Module used to automatically load data into multiple fake S3 buckets"""
import os
import json
import logging
from datetime import datetime
from typing import Any, cast, Optional
from pydantic import BaseModel
from pytz import UTC
from mypy_boto3_s3 import S3Client
import moto  # type: ignore
from base.aws.s3 import S3Controller

_logger = logging.getLogger(__name__)


class MockS3File(BaseModel):
    """Represents a File in S3"""
    data: bytes = b"MOCK"
    s3_path: str
    date_modified: datetime = datetime.now(tz=UTC)


class S3StateLoader:
    """
    This module is designed for the automated loading of data into multiple simulated S3 buckets,
    facilitating the replication of a file structure within S3.


    To initiate the loader, two paths need to be provided: one containing state files (state_folder_path)
    and another containing the content to be stored in S3 (file_content_path).

    THE STATE_FILE:
    The state file is a JSON file comprising a list of MockS3File instances.
    Each MockS3File represents a file in S3. For each file, the loader searches for the corresponding file name
    (based on the s3_file) in the file_content_path. If the file exists, it is loaded to S3;
    if not, a mock file with the same key is uploaded.

    Additionally, the date_modified can be customized in the state file.
    If it is not specified, the modified_date will be set to the current day.



    """

    def __init__(self, state_folder_path: str, file_content_path: str, s3_client: S3Client):
        """
        Constructor, stores the paths necessary for subsequent calls.

        Args:
            state_folder_path (str): The path to the s3 state
            file_content_path (str): The path for the files content.
        """
        if file_content_path == "":
            self.file_content_path = os.path.join(state_folder_path)
        else:
            self.file_content_path = file_content_path

        self.state_folder_path = state_folder_path
        self.s3_client = s3_client

    @staticmethod
    def __load_json_file(file_path: str) -> dict:
        """Load a json file"""
        with open(file_path, "r", encoding="utf-8") as json_file:
            return json.load(json_file)

    def __get_s3_cloud_state(self, filename: str) -> list[MockS3File]:
        """
        Loads a snapshot of an s3 bucket from the directory self.state_folder_path.
        The structure of the file should be a json containing a list of "MockS3File" that can be
        later used to the S3 of RCC or DevCloud.
        If any of the fields are missing in each MockS3File, it will be replaced for a default value.
        In addition to this, the file name will be searched
        in self.file_content_path, and if it is found it will load the contents into the data field.

        Args:
            filename (str): The file to be loaded (Without the path)

        Raises:
            Exception: If the field "data" exists in the file (not supported yet)
        Returns:
            list[MockS3File]: A list of files to be mocked.
        """
        state_file_path = os.path.join(self.state_folder_path, filename)
        state: list[dict[Any, Any]] = cast(list[dict[Any, Any]], self.__load_json_file(state_file_path))
        result: list[MockS3File] = []
        for s3_file_dict in state:
            s3_data = MockS3File.model_validate(s3_file_dict)
            if content := self.get_s3_file_content(s3_data.s3_path):
                s3_data.data = content

            result.append(s3_data)

        return result

    def get_s3_file_content(self, s3_path: str) -> Optional[bytes]:
        """
        Loads a file from the file_content_path directory.
        The filename will be stripped out of the s3_path and will be used
        for searching the file contents contained in the directory hold by
        self.file_content_path.

        A filename can also be passed as arugment instead of an s3_path.

        Args:
            s3_path (str): The name of the file to be loaded or the s3 path.

        Returns:
            Optional[bytes]: The content of the file if it can be loaded
        """
        filename = s3_path.split("/")[-1]
        path = os.path.join(self.file_content_path, filename)
        if not os.path.isfile(path):
            return None
        with open(os.path.join(self.file_content_path, filename), "rb") as file_content:
            return file_content.read()

    def load_s3_state(self, filename: str):
        """
        Load an s3 state into a mocked client

        Args:
            filename (str): The file name within the state directory to be loaded.
            s3_client (S3Client): _description_
        """

        files_to_load = self.__get_s3_cloud_state(filename)

        for file_to_load in files_to_load:
            bucket, key = S3Controller.get_s3_path_parts(file_to_load.s3_path)
            _logger.info("Loading file %s in bucket %s with data_size=%d", key, bucket, len(file_to_load.data))
            self.s3_client.put_object(Bucket=bucket, Key=key, Body=file_to_load.data)

            # Replace date modified
            s3_obj = moto.s3.models.s3_backends["123456789012"]["global"].buckets[bucket].keys[key]
            s3_obj.last_modified = file_to_load.date_modified
