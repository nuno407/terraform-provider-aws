import pytest
from botocore.errorfactory import ClientError
from base.aws.s3 import S3Controller
from unittest.mock import Mock
from typing import Union
from mypy_boto3_s3 import S3Client


@pytest.mark.unit
@pytest.mark.parametrize("bucket,path,response,expected", [
    (
        "foobucket",
        "exists",
        {
            "ResponseMetadata": {
                "HTTPStatusCode": 200
            }
        },
        True
    ),
    (
        "foobucket",
        "do-not-exist",
        {
            "ResponseMetadata": {
                "HTTPStatusCode": 404
            }
        },
        False
    )
])
def test_check_s3_file_exists(bucket: str, path: str, response: dict, expected: bool, s3_controller: S3Controller, s3_client: Mock):
    s3_client.head_object = Mock(return_value=response)
    assert s3_controller.check_s3_file_exists(
        bucket, path) == expected
    s3_client.head_object.assert_called_once_with(Bucket=bucket, Key=path)


@pytest.mark.unit
def test_check_s3_file_exists_client_error(s3_controller: S3Controller, s3_client: Mock):
    s3_client.head_object = Mock(side_effect=ClientError(
        {"Error": {"Code": "404"}}, "head_object"))
    assert s3_controller.check_s3_file_exists(
        "testbucket", "testpath") == False


@pytest.mark.unit
@pytest.mark.parametrize("input_path,expected", [
    (
        "s3://bucket/some/path/to/file.json",
        ("bucket", "some/path/to/file.json")
    ),
    (
        "s3://bucket/file.json",
        ("bucket", "file.json")
    ),
    (
        "/bucket/some/path/to/file.json",
        ValueError
    ),
    (
        "s3:/bucket/file.json",
        ValueError
    ),
])
def test_get_s3_path_parts(input_path: str, expected: Union[Exception, tuple[str, str]], s3_controller: S3Controller):

    if isinstance(expected, tuple):
        assert s3_controller.get_s3_path_parts(input_path) == expected
    else:
        with pytest.raises(expected):
            s3_controller.get_s3_path_parts(input_path)
        return
