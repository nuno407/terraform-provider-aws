import json

from kink import inject
from mypy_boto3_sns import SNSClient

from base.aws.model import SQSMessage


@inject
class SNSController:
    """" AWS SNS message controller. """
    def __init__(self,
                sns_client: SNSClient):
        self.__sns_client = sns_client

    def publish(self, topic_arn: str, message: SQSMessage) -> None:
        """ Publishes message into a topic """
        self.__sns_client.publish(
            TopicArn=topic_arn,
            Message=json.dumps({"default": json.dumps(message.body)}),
            MessageAttributes={
                "tenant": {
                    "DataType": "String",
                    "StringValue": message.attributes.tenant
                },
                "device_id": {
                    "DataType": "String",
                    "StringValue": message.attributes.device_id
                }
            },
            MessageStructure="json"
        )
