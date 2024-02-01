""" Test SQSMessage Class """
import pytest

from data_importer.sqs_message import SQSMessage

# pylint: disable=missing-function-docstring, missing-module-docstring, missing-class-docstring, too-few-public-methods


def _sqs_message_helper(principal_id, bucket_name, object_key):
    return {"Body": str({
        "Records": [
            {
                "userIdentity": {
                    "principalId": principal_id
                },
                "s3": {
                    "bucket": {
                        "name": bucket_name
                    },
                    "object": {
                        "key": object_key
                    }
                }
            }
        ]
    })
    }


@pytest.mark.unit
class TestSQSMessage():

    @pytest.mark.parametrize("sqs_message,expected_sqs_message", [
        (
            _sqs_message_helper("mock_id", "dev-mock_bucket-raw", "samples/mock_key/mock_file.mock_extension"),
            SQSMessage(principal_id="mock_id",
                       bucket_name="dev-mock_bucket-raw",
                       file_path="samples/mock_key/mock_file.mock_extension",
                       file_extension="mock_extension",
                       dataset="mock_key",
                       tenant_id="MOCK_BUCKET")
        ),
        (
            _sqs_message_helper("mock_id", "dev-mock_bucket-raw", "samples/mock_file.mock_extension"),
            SQSMessage(principal_id="mock_id",
                       bucket_name="dev-mock_bucket-raw",
                       file_path="samples/mock_file.mock_extension",
                       file_extension="mock_extension",
                       dataset="default",
                       tenant_id="MOCK_BUCKET")
        )
    ]
    )
    def test_from_raw_sqs_message(self, sqs_message, expected_sqs_message: SQSMessage):
        test_sqs_message = SQSMessage.from_raw_sqs_message(sqs_message)

        assert test_sqs_message.principal_id == expected_sqs_message.principal_id, "wrong principal id"
        assert test_sqs_message.bucket_name == expected_sqs_message.bucket_name, "wrong bucket"
        assert test_sqs_message.file_path == expected_sqs_message.file_path, "wrong file_path"
        assert test_sqs_message.file_extension == expected_sqs_message.file_extension, "wrong file_extension"
        assert test_sqs_message.dataset == expected_sqs_message.dataset, "wrong dataset"
        assert test_sqs_message.full_path == expected_sqs_message.full_path, "wrong full path"
