""" SNS module. """
import json
import logging

from botocore.exceptions import ClientError
from kink import inject
from mypy_boto3_sns import SNSClient

_logger = logging.getLogger(__name__)


@inject
class SNSController:  # pylint: disable=too-few-public-methods
    """" AWS SNS message controller. """

    def __init__(self,
                 sns_client: SNSClient):
        self.__sns_client = sns_client

    @inject(bind={"sns_topic_arn": "default_sns_topic_arn"})
    def publish(self, message: str, sns_topic_arn: str) -> None:
        """ Publishes message into a topic """
        try:
            response = self.__sns_client.publish(
                TopicArn=sns_topic_arn,
                Message=json.dumps({"default": json.loads(message)}),
                MessageStructure="json"
            )
            message_id = response["MessageId"]
            _logger.info("Published message to topic %s. MessageId: %s", sns_topic_arn, message_id)
        except ClientError:
            _logger.exception("Couldn't publish message to topic %s.", sns_topic_arn)
            raise
