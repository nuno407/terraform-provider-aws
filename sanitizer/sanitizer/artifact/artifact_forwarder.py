""" artifact forwarder module. """
import json

from kink import inject

from base.aws.sns import SNSController
from base.model.artifacts import Artifact


@inject
class ArtifactForwarder: # pylint: disable=too-few-public-methods
    """ Publishes artifact message to output topic """

    def __init__(self, aws_sns_controller: SNSController) -> None:
        self.aws_sns_controller = aws_sns_controller

    def publish(self, artifact: Artifact) -> None:
        """ publishes artifact to output topic. """
        # convert to sqs message and send to sns
        self.aws_sns_controller.publish(json.dumps(artifact.__dict__))
