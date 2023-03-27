""" artifact forwarder module. """
from kink import inject
from sanitizer.model import Artifact

from sanitizer.aws.sns import AWSSNSController


@inject
class ArtifactForwarder:
    """ Publishes artifact message to output topic """
    def __init__(self, aws_sns_controller: AWSSNSController) -> None:
        self.aws_sns_controller = aws_sns_controller

    def publish(self, artifact: Artifact) -> None:
        """ publishes artifact to output topic. """
        raise NotImplementedError("TODO")
