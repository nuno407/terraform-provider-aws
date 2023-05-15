""" VideoParser class """
import json
from typing import Iterator, Optional

from kink import inject

from base.aws.model import SQSMessage
from base.model.artifacts import RecorderType, VideoArtifact, TimeWindow
from base.timestamps import from_epoch_seconds_or_milliseconds
from sanitizer.artifact.parsers.iparser import IArtifactParser
from sanitizer.exceptions import InvalidMessageError


@inject
class VideoParser(IArtifactParser):  # pylint: disable=too-few-public-methods
    """VideoParser class"""

    def parse(self, sqs_message: SQSMessage, recorder_type: RecorderType) -> Iterator[VideoArtifact]:
        """Extract recording artifact from SQS message

        Args:
            message (SQSMessage): incoming SQS message

        Raises:
            InvalidMessageError: error parsing the incoming message

        Returns:
            Artifact: Recording artifact
        """
        if not sqs_message.body:
            raise InvalidMessageError("Invalid message, empty body.")

        inner_message: Optional[dict] = sqs_message.body.get("Message")
        if not inner_message:
            raise InvalidMessageError(
                "Invalid message body. Cannot extract message contents.")

        if isinstance(inner_message, str):
            inner_message = json.loads(inner_message)
            if not inner_message:
                raise InvalidMessageError(
                    "Invalid message body. Cannot extract message contents.")

        stream_name = inner_message.get("streamName")
        if not stream_name:
            raise InvalidMessageError(
                "Invalid message body. Cannot extract stream name.")

        footage_from = inner_message.get("footageFrom")
        if not footage_from:
            raise InvalidMessageError(
                "Invalid message body. Cannot extract footageFrom.")

        footage_to = inner_message.get("footageTo")
        if not footage_to:
            raise InvalidMessageError(
                "Invalid message body. Cannot extract footageTo.")

        upload_started = inner_message.get("uploadStarted")
        if not upload_started:
            raise InvalidMessageError(
                "Invalid message body. Cannot extract uploadStarted.")

        upload_finished = inner_message.get("uploadFinished")
        if not upload_finished:
            raise InvalidMessageError(
                "Invalid message body. Cannot extract uploadFinished.")

        tenant = sqs_message.attributes.tenant
        if not tenant:
            raise InvalidMessageError(
                "Invalid message attribute. Cannot extract tenant.")

        device = sqs_message.attributes.device_id
        if not device:
            raise InvalidMessageError(
                "Invalid message attribute. Cannot extract deviceId.")

        yield (VideoArtifact(
            tenant_id=tenant,
            device_id=device,
            stream_name=stream_name,
            recorder=recorder_type,
            timestamp=from_epoch_seconds_or_milliseconds(footage_from),
            end_timestamp=from_epoch_seconds_or_milliseconds(footage_to),
            upload_timing=TimeWindow(
                start=from_epoch_seconds_or_milliseconds(upload_started),
                end=from_epoch_seconds_or_milliseconds(upload_finished)
            )))
