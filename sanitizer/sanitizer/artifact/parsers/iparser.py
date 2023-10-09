""" ArtifactParser interface. """
from typing import Iterator, Optional

from base.aws.model import SQSMessage
from base.model.artifacts import Artifact, RecorderType
from sanitizer.exceptions import InvalidMessageError


class IArtifactParser:  # pylint: disable=too-few-public-methods
    """ ArtifactParser interface. """

    def parse(self, sqs_message: SQSMessage, recorder_type: Optional[RecorderType]) -> Iterator[Artifact]:
        """ Parse SQS message and return list of artifacts. """
        raise NotImplementedError

    def _check_attribute_not_none(self, value, attribute_name: str) -> None:
        if value is None:
            raise InvalidMessageError("Invalid message body. Cannot extract " + attribute_name + ".")

    def _check_recorder_not_none(self, recorder: Optional[RecorderType]) -> None:
        if recorder is None:
            raise InvalidMessageError("Invalid message body. Cannot extract recorder.")
