from moto import mock_s3, mock_sqs
from mypy_boto3_s3 import S3Client
from typing import Generator
import boto3
import pytest


@pytest.fixture()
def region() -> str:
    return "us-east-1"


@pytest.fixture()
def moto_s3_client(region: str) -> Generator[S3Client, None, None]:
    """

    Returns:
        Mock: mocked S3 client
    """
    with mock_s3():
        moto_client = boto3.client("s3", region_name=region)

        yield moto_client
