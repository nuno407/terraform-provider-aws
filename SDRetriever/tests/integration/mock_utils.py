from datetime import datetime
from pydantic import BaseModel
from mypy_boto3_sqs.client import SQSClient
from mypy_boto3_s3 import S3Client
from base.aws.s3 import S3Controller
from pytz import UTC
import os
import moto  # type: ignore
import json
import random
from base.aws.sqs import SQSController
from base.model.artifacts import Artifact, parse_artifact
from typing import Any, cast, Optional
from mypy_boto3_sqs.type_defs import MessageTypeDef
from base.testing.utils import load_relative_raw_file, load_relative_json_file, get_abs_path, load_relative_str_file
from sdretriever.main import deserialize


class S3File(BaseModel):
    data: bytes = b"MOCK"
    date_modified: datetime = datetime.now(tz=UTC)
    file_name: str = f"{random.getrandbits(128)}.dummy"
    device_id: str = "dummy_device"
    tenant: str = "dummy_tenant"


def get_s3_file_content(filename: str) -> bytes:
    """
    Loads a file from the directory data/cloud_s3_state/files_content

    Args:
        filename (str): The name of the file to be loaded (Without the path)

    Returns:
        bytes: The content of the file
    """
    return load_relative_raw_file(__file__, os.path.join("data", "cloud_s3_state", "files_content", filename))


def get_s3_cloud_state(filename: str) -> list[S3File]:
    """
    Loads a snapshot of an s3 bucket from the directory data/cloud_s3_state.
    The structure of the file should be a json containing a list of "S3File" that can be
    later used to the S3 of RCC or DevCloud.
    If any of the fields are missing in each S3File, it will be replaced for a default value.
    In addition to this, if the file_name is present in an S3File, that filename will be searched
    in data/cloud_s3_state/files_content, and if it is found it will load the contents into the data field.

    Args:
        filename (str): The file to be loaded (Without the path)

    Raises:
        Exception: If the field "data" exists in the file (not supported yet)
    Returns:
        list[S3File]: A list of files to be mocked.
    """
    data: list[dict[Any, Any]] = cast(list[dict[Any, Any]], load_relative_json_file(
        __file__, os.path.join("data", "cloud_s3_state", filename)))
    result: list[S3File] = []
    for s3_file_dict in data:
        if "data" in s3_file_dict:
            raise Exception("Inline data loading for s3 cloud state not supported")

        # Try to load content file in files_content
        elif "file_name" in s3_file_dict:
            path_to_file_content = get_abs_path(
                __file__,
                os.path.join(
                    "data",
                    "cloud_s3_state",
                    "files_content",
                    s3_file_dict["file_name"]))
            if os.path.isfile(path_to_file_content):
                s3_file_dict["data"] = get_s3_file_content(s3_file_dict["file_name"])

        result.append(S3File(**s3_file_dict))

    return result


def get_sqs_message(filename: str) -> str:
    """
    Load an sqs message in the directory "data/sqs_messages"

    Args:
        filename (str): The file to be loaded (Without the path)

    Returns:
        str: The message as a string format
    """
    data: str = load_relative_str_file(__file__, os.path.join("data", "sqs_messages", filename))
    return data


def get_local_content_from_s3_path(path: str) -> bytes:
    """
    Loads a file from the directory data/cloud_s3_state/files_content

    Args:
        path (str): The name S3 path

    Returns:
        bytes: The content of the file
    """
    file_name = path.split("/")[-1]
    return get_s3_file_content(file_name)


def get_sqs_message_artifact(sqs_controller: SQSController, timeout: int) -> Optional[Artifact]:
    """
    Get's a message from an SQS queue and returns it's artifact if available

    Args:
        sqs_controller (SQSController): The Controller

    Returns:
        Optional[Artifact]
    """
    sqs_message = sqs_controller.get_message(timeout)
    if sqs_message:
        metadata_message = json.loads(deserialize(sqs_message["Body"]))
        return parse_artifact(metadata_message)
    return None


def load_files_rcc_chunks(files_to_load: list[S3File], rcc_s3_client: S3Client, rcc_bucket: str):
    """
    Load a list of S3Files to a mocked S3Client to the specified bucket.
    This will mimick the file structure of the RCC, so the path will be set based on the modified date.

    Args:
        files_to_load (list[S3File]): The files to be loaded to RCC
        rcc_s3_client (S3Client): The mocked S3 client.
        rcc_bucket (str): The RCC bucket.
    """
    for file_to_load in files_to_load:
        year = file_to_load.date_modified.year
        month = file_to_load.date_modified.month
        day = file_to_load.date_modified.day
        hour = file_to_load.date_modified.hour

        rcc_key: str = f"{file_to_load.tenant}/{file_to_load.device_id}/year={
            year}/month={month:02d}/day={day:02d}/hour={hour:02d}/{file_to_load.file_name}"
        rcc_s3_client.put_object(Bucket=rcc_bucket, Key=rcc_key, Body=file_to_load.data)
        key = moto.s3.models.s3_backends["123456789012"]["global"].buckets[rcc_bucket].keys[rcc_key]
        key.last_modified = file_to_load.date_modified


def load_files(file_s3_path: list[str], s3_client: S3Client):
    """
    Load a list of S3 paths to a mocked S3Client to the specified bucket.
    The files will be grabbed from "files_content" folder.

    Args:
        file_s3_path (list[str]): The files to be loaded to S3
        s3_client (S3Client): The mocked S3 client.
    """
    for file_path in file_s3_path:
        bucket, file_key = S3Controller.get_s3_path_parts(file_path)
        content = get_local_content_from_s3_path(file_path)
        s3_client.put_object(Bucket=bucket, Key=file_key, Body=content)


def load_files_devcloud(files_to_load: list[S3File], devcloud_s3_client: S3Client, devcloud_bucket: str):
    """
    Load a list of S3Files to a mocked S3Client to the specified bucket.
    This will mimick the file structure of the DevCloud.

    Args:
        files_to_load (list[S3File]): The files to be loaded to DevCloud
        rcc_s3_client (S3Client): The mocked S3 client.
        rcc_bucket (str): The DevCloud bucket.
    """
    for file_to_load in files_to_load:
        dev_cloud_key: str = f"{file_to_load.tenant}/{file_to_load.file_name}"
        devcloud_s3_client.put_object(Bucket=devcloud_bucket, Key=dev_cloud_key, Body=file_to_load.data)
        key = moto.s3.models.s3_backends["123456789012"]["global"].buckets[devcloud_bucket].keys[dev_cloud_key]
        key.last_modified = file_to_load.date_modified


def load_sqs_message(data: str, sqs_client: SQSController):
    """
    Loads an sqs message into a queue.

    Args:
        data (str): The data to be sent into the queue.
        sqs_client (SQSClient): The mocked SQS client.
        sqs_name (str): The SQS name.
    """
    message_wrapper = {"Message": data}
    sqs_client.send_message(json.dumps(message_wrapper), "SDRetriever")
