""" Test SQSMessage Class """
from data_importer.sqs_message import SQSMessage
import pytest
import unittest


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


class TestSQSMessage():

    @pytest.mark.unit
    @pytest.mark.parametrize("sqs_message,expected_sqs_message", [
        (
            _sqs_message_helper("mock_id", "mock_bucket", "mock_key/mock_file.mock_extension"),
            SQSMessage(principal_id="mock_id",
                       bucket_name="mock_bucket",
                       file_path="mock_key/mock_file.mock_extension",
                       file_extension="mock_extension",
                       dataset="mock_key")
        ),
        (
            _sqs_message_helper("mock_id", "mock_bucket", "mock_file.mock_extension"),
            SQSMessage(principal_id="mock_id",
                       bucket_name="mock_bucket",
                       file_path="mock_file.mock_extension",
                       file_extension="mock_extension",
                       dataset="default")
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
