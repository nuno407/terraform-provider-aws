"""Test main"""
import json
from unittest import mock
from unittest.mock import ANY, Mock, call

import pytest

from data_importer.main import main, process_message

# pylint: disable=missing-function-docstring, missing-module-docstring, missing-class-docstring, too-few-public-methods
# pylint: disable=redefined-outer-name


def message(extension):
    return {
        "MessageId": "037bc5ef-c907-4520-b89d-3854578c29db",
        "ReceiptHandle": "test-handle",
        "MD5OfBody": "18fa90314e61030f6b5116b0db6b3c6e",
        "Body":
            str(
                {"Records": [{"eventVersion": "2.1", "eventSource": "aws:s3", "awsRegion": "eu-central-1",
                              "eventTime": "2023-02-21T17:25:40.334Z", "eventName": "ObjectCreated:Put",
                              "userIdentity": {"principalId": "AWS:SOMETHING:bumlux@bosch.com"},
                              "requestParameters": {"sourceIPAddress": "213.61.69.130"},
                              "responseElements": {"x-amz-request-id": "54ADJ1QBN2GR8GXN",
                                                   "x-amz-id-2": "something"},
                              "s3": {"s3SchemaVersion": "1.0", "configurationId": "test-event",
                                     "bucket": {"name": "dev-mock_bucket-raw",
                                                "ownerIdentity": {"principalId": "SOMETHING"},
                                                "arn": "arn:aws:s3:::dev-mock_bucket-raw"},
                                     "object": {"key": f"samples/test-dataset/bumlux.{extension}",
                                                "size": 4043, "eTag": "2712b08adeeecf9ab9fda9beec1d6adf",
                                                "sequencer": "0063F4FE942D3BC51D"}}}]}
            ),
        "Attributes": {
            "SentTimestamp": "1677000341187",
            "ApproximateReceiveCount": "1"}
    }


@pytest.fixture()
def metadata():
    return json.dumps({
        "foo": "bar",
        "test-data": "yes"
    })


@pytest.mark.unit
class TestMain:

    @mock.patch("boto3.client")
    @mock.patch("data_importer.main.ContainerServices")
    @mock.patch("data_importer.main.process_message")
    def test_main(self, process_message: Mock, _container_services, client: Mock):
        # GIVEN
        _stop_condition = Mock(side_effect=[True, False])

        # WHEN
        main(stop_condition=_stop_condition)

        # THEN
        client.assert_called()
        client.assert_called()
        assert 2 == client.call_count
        process_message.assert_called_once()

    @pytest.mark.parametrize("image_message, json_message", [(message("jpg"), message("json"))])
    def test_processing_image_message(self, image_message, json_message, metadata):
        # GIVEN
        container_services = Mock()
        container_services.get_single_message_from_input_queue = Mock(side_effect=[image_message, json_message])
        container_services.download_file = Mock(return_value=metadata)
        importer = Mock()
        s3_client = Mock()
        sqs_client = Mock()
        dataset = Mock()
        importer.load_dataset = Mock(return_value=dataset)

        # WHEN
        process_message(container_services, importer, s3_client, sqs_client)
        process_message(container_services, importer, s3_client, sqs_client)

        # THEN
        expected_file_path = "s3://dev-mock_bucket-raw/samples/test-dataset/bumlux."
        expected_image = expected_file_path + "jpg"
        expected_json = expected_file_path + "json"
        importer.load_dataset.assert_has_calls(
            [call("MOCK_BUCKET-test-dataset", ["MOCK_BUCKET"]), call("MOCK_BUCKET-test-dataset", ["MOCK_BUCKET"])])
        importer.upsert_sample.assert_called_once_with(
            "MOCK_BUCKET", dataset, expected_image, {
                "filepath": expected_image, "metadata": ANY})
        importer.replace_sample.assert_called_once_with(
            "MOCK_BUCKET", dataset, expected_json, {
                "foo": "bar", "test-data": "yes"})
        container_services.delete_message.assert_called_with(sqs_client, "test-handle", input_queue=ANY)
