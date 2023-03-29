""" SNS module. """
import json

from mypy_boto3_sns import SNSClient
from kink import inject

@inject
class SNSController: # pylint: disable=too-few-public-methods
    """" AWS SNS message controller. """
    def __init__(self,
                sns_client: SNSClient):
        self.__sns_client = sns_client

    @inject(bind={"sns_topic_arn":"default_sns_topic_arn"})
    def publish(self, message: str, sns_topic_arn: str) -> None:
        """ Publishes message into a topic """

        self.__sns_client.publish(
            TopicArn=sns_topic_arn,
            Message=json.dumps({"default": message}),
            MessageStructure="json"
        )
