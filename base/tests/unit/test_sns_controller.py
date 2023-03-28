import pytest
from base.aws.sns import SNSController
from unittest.mock import Mock
from base.aws.model import SQSMessage, MessageAttributes

# @pytest.mark.unit
# @pytest.mark.parametrize("topic_arn,sqs_message", [
#     (
#         "arn:aws:sns:us-east-1:123456789012:MyTopic",
#         SQSMessage(
#             body={"foo": "bar"},
#             attributes=None
#         )
#     ),
#     (
#         "arn:aws:sns:us-east-1:123456789012:MyTopic2",
#         SQSMessage(
#             body={"foo": "bar"},
#             attributes=MessageAttributes(
#                 tenant="tenant",
#                 device_id="device_id"
#             )
#         )
#     )
# ])
# def test_sns_publish(topic_arn: str, sqs_message: SQSMessage):
#     sns_client = Mock()
#     sns_client.publish = Mock()
#     sns_controller = SNSController(sns_client)
#     sns_controller.publish(topic_arn, sqs_message)
#     sns_client.publish.assert_called_once_with(
#         TopicArn=topic_arn,
#         Message=sqs_message.body,
#         MessageAttributes=sqs_message.attributes
#     )
