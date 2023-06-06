from moto import mock_s3, mock_sqs
from mypy_boto3_s3 import S3Client
from base.aws.model import S3ObjectInfo
from base.aws.s3 import S3Controller
from datetime import datetime
import boto3
import pytest
import os
from typing import Iterator


def helper_fill_dummy_bucket(s3_client: S3Client, path: str, n_files: int, bucket_name: str):
    s3_client.create_bucket(Bucket=bucket_name)

    for i in range(n_files):
        key = os.path.join(path, f"file_{i}.bin")
        s3_client.put_object(Key=key, Bucket=bucket_name, Body=b"MOCK")


def expected_dummy_data(path: str, n_files: int) -> list[S3ObjectInfo]:
    result: list[S3ObjectInfo] = list()
    for i in range(n_files):
        key = os.path.join(path, f"file_{i}.bin")
        obj = S3ObjectInfo(key=key, date_modified=datetime.now(), size=4)
        result.append(obj)

    return result


class TestS3Controller:

    @pytest.mark.integration
    @pytest.mark.parametrize("path,n_files,iterations,expected_objects", [
        (
            "/some/bucket/",
            100,
            1,
            expected_dummy_data("/some/bucket/", 100)
        ), (
            "/some/bucket/",
            1001,
            None,
            expected_dummy_data("/some/bucket/", 1001)
        ),

    ])
    def test_list_directory_objects(self, moto_s3_client: S3Client, path: str,
                                    n_files: int, iterations: int, expected_objects: list[S3ObjectInfo]):

        # GIVEN
        bucket_name = "random"
        expected_objects_keys = {obj.key for obj in expected_objects}
        expected_objects_size = {obj.size for obj in expected_objects}
        helper_fill_dummy_bucket(moto_s3_client, path, n_files, bucket_name)
        controller = S3Controller(moto_s3_client)

        # WHEN

        if iterations:
            objects_iter = controller.list_directory_objects(
                path, bucket_name, max_iterations=iterations)
        else:
            objects_iter = controller.list_directory_objects(
                path, bucket_name)
        objects_set = set(objects_iter)

        # THEN
        assert isinstance(objects_iter, Iterator)
        assert len(objects_set) == len(expected_objects)

        for obj in objects_set:
            assert obj.key in expected_objects_keys
            assert obj.size in expected_objects_size
            assert isinstance(obj.date_modified, datetime)
            assert obj.get_file_name() == obj.key.split("/")[-1]
