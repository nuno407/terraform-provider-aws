import json
import logging
import os
from logging import Logger
from unittest.mock import ANY, MagicMock, Mock, PropertyMock, call

from kink import di
from mypy_boto3_sns import SNSClient
from mypy_boto3_sqs import SQSClient
from pymongo import MongoClient
from pytest import mark

from base.aws.sqs import SQSController
from base.graceful_exit import GracefulExit
from base.model.artifacts import IncidentEventArtifact, parse_artifact
from base.model.event_types import IncidentType
from sanitizer.config import SanitizerConfig
from sanitizer.handler import Handler

INPUT_QUEUE = "input_queue"
METADATA_QUEUE = "metadata_queue"
TOPIC_ARN = "topic_arn"
MESSAGE_COLLECTION = "message_collection"
DB_NAME = "db"

CURRENT_LOCATION = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))
TEST_DATA = os.path.join(CURRENT_LOCATION, "data")


@mark.integration
class TestSanitizerIntegration:

    def setup_method(self, method):
        di.clear_cache()

        di["start_delay"] = 0
        di["container_name"] = "test-sanitizer"

        config = SanitizerConfig(input_queue=INPUT_QUEUE,
                                 metadata_queue=METADATA_QUEUE,
                                 topic_arn=TOPIC_ARN,
                                 message_collection=MESSAGE_COLLECTION,
                                 db_name=DB_NAME,
                                 tenant_blacklist=[],
                                 recorder_blacklist=[],
                                 device_info_collection="device_info_collection",
                                 version_blacklist={},
                                 devcloud_raw_bucket="test-raw",
                                 devcloud_anonymized_bucket="test-anonymized")
        di[SanitizerConfig] = config
        di["default_sns_topic_arn"] = config.topic_arn

        di[Logger] = logging.getLogger("sanitizer")
        one_time_to_exit = Mock()
        type(one_time_to_exit).continue_running = PropertyMock(side_effect=[True, False])
        di[GracefulExit] = one_time_to_exit

        di[MongoClient] = MagicMock()

        di[SQSClient] = MagicMock()
        di[SQSClient].get_queue_url.side_effect = [{"QueueUrl": METADATA_QUEUE}, {"QueueUrl": INPUT_QUEUE}]
        di[SNSClient] = Mock()

        di["metadata_sqs_controller"] = SQSController(
            default_sqs_queue_name=config.metadata_queue,
            sqs_client=di[SQSClient])
        di["aws_sqs_rcc_controller"] = SQSController(
            default_sqs_queue_name=config.input_queue,
            sqs_client=di[SQSClient])

    def teardown_method(self, method):
        di.clear_cache()

    def test_ingest_event(self):
        # GIVEN
        data_path = os.path.join(TEST_DATA, "valid_incident_event.json")
        with open(data_path) as fp:
            message = json.loads(fp.read())

        di["aws_sqs_rcc_controller"].get_message = Mock(return_value=message)

        # WHEN
        handler = di[Handler]
        handler.run()

        # THEN
        print(di[SQSClient].get_queue_url.call_args_list)
        di[SQSClient].get_queue_url.assert_has_calls([call(QueueName=METADATA_QUEUE), call(QueueName=INPUT_QUEUE)])
        di[SQSClient].send_message.assert_called_once_with(QueueUrl=METADATA_QUEUE, MessageBody=ANY, MessageAttributes={
            "SourceContainer": {"StringValue": "test-sanitizer", "DataType": "String"}})
        sent_artifact_str = di[SQSClient].send_message.call_args.kwargs["MessageBody"]
        event_artifact = parse_artifact(sent_artifact_str)
        assert isinstance(event_artifact, IncidentEventArtifact)
        assert event_artifact.device_id == "DATANAUTS_DEV_01"
        assert event_artifact.incident_type == IncidentType.ACCIDENT_AUTO
        # just some basic assertions here, since the deep check is done in the parser unit test

        # verify the message did not get published on SNS
        assert di[SNSClient].publish.call_count == 0
