import pytest
import json
from base.aws.sns import SNSController
from unittest.mock import Mock


@pytest.mark.unit
@pytest.mark.parametrize("topic_arn,message", [
    (
        "arn:aws:sns:us-east-1:123456789012:MyTopic",
        """
        {
            "message": "Hello world!"
        }
        """
    ),
    (
        "arn:aws:sns:us-east-1:123456789012:MyTopic2",
        """
        {
            "device_id": "device-1234",
            "tenant_id": "datanauts",
            "recorder_type": "InteriorRecorder"
        }
        """
    )
])
def test_sns_publish(topic_arn: str, message: str):
    sns_client = Mock()
    sns_client.publish = Mock(return_value={"MessageId": "1234"})
    sns_controller = SNSController(sns_client)
    sns_controller.publish(message, topic_arn)
    sns_client.publish.assert_called_once_with(
        TopicArn=topic_arn,
        Message=json.dumps({"default": message}),
        MessageStructure="json"
    )
