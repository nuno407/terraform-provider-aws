""" ArtifactParser interface. """
from typing import Iterator, Protocol

from base.aws.model import SQSMessage
from base.model.artifacts import VideoArtifact


class IArtifactParser(Protocol):  # pylint: disable=too-few-public-methods
    """ ArtifactParser interface. """

    def parse(self, sqs_message: SQSMessage) -> Iterator[VideoArtifact]:
        """ Parse SQS message and return list of artifacts. """
