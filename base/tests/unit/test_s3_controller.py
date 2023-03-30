import pytest
from botocore.errorfactory import ClientError
from base.aws.s3 import S3Controller
from unittest.mock import Mock


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
def test_check_s3_file_exists(bucket: str, path: str, response: dict, expected: bool):
    s3_client = Mock()
    s3_client.head_object = Mock(return_value=response)
    assert S3Controller(s3_client).check_s3_file_exists(bucket, path) == expected
    s3_client.head_object.assert_called_once_with(Bucket=bucket, Key=path)


def test_check_s3_file_exists_client_error():
    s3_client = Mock()
    s3_client.head_object = Mock(side_effect=ClientError({"Error": {"Code": "404"}}, "head_object"))
    s3_controller = S3Controller(s3_client)
    assert s3_controller.check_s3_file_exists("testbucket", "testpath") == False
