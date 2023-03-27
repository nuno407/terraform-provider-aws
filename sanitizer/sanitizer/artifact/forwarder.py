""" artifact forwarder module. """
from kink import inject
from sanitizer.model import Artifact

from base.aws.model import SQSMessage, MessageAttributes
from base.aws.sns import SNSController


@inject
class ArtifactForwarder:
    """ Publishes artifact message to output topic """

    def __init__(self, aws_sns_controller: SNSController) -> None:
        self.aws_sns_controller = aws_sns_controller

    def publish(self, artifact: Artifact) -> None:
        """ publishes artifact to output topic. """
        # convert to sqs message and send
        message = SQSMessage(
            body=dict(artifact),
            attributes=MessageAttributes(
                tenant=artifact.tenant_id,
                device_id=artifact.device_id))
        self.aws_sns_controller.publish(message)
