""" artifact forwarder module. """

import logging

from kink import inject

from base.aws.sns import SNSController
from base.model.artifacts import Artifact

_logger = logging.getLogger(__name__)


@inject
class ArtifactForwarder:  # pylint: disable=too-few-public-methods
    """ Publishes artifact message to output topic """

    def __init__(self, aws_sns_controller: SNSController) -> None:
        self.aws_sns_controller = aws_sns_controller

    def publish(self, artifact: Artifact) -> None:
        """ publishes artifact to output topic. """
        # convert to raw JSON message and send to sns
        raw_message = artifact.stringify()
        self.aws_sns_controller.publish(raw_message)
