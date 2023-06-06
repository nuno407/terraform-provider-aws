""" VideoParser class """
from typing import Iterator

from kink import inject

from base.aws.model import SQSMessage
from base.model.artifacts import KinesisVideoArtifact, RecorderType, TimeWindow
from base.timestamps import from_epoch_seconds_or_milliseconds
from sanitizer.artifact.parsers.video_parser import VideoParser


@inject
class KinesisVideoParser(VideoParser):  # pylint: disable=too-few-public-methods
    """VideoParser class"""

    def parse(self, sqs_message: SQSMessage, recorder_type: RecorderType) -> Iterator[KinesisVideoArtifact]:
        """Extract recording artifact stored on Kinesis from SQS message

        Args:
            message (SQSMessage): incoming SQS message

        Raises:
            InvalidMessageError: error parsing the incoming message

        Returns:
            Artifact: Recording artifact
        """
        inner_message: dict = self._get_inner_message(sqs_message)

        stream_name = inner_message.get("streamName")
        self._check_attribute_not_none(stream_name, "stream name")

        footage_from = inner_message.get("footageFrom")
        self._check_attribute_not_none(footage_from, "footageFrom")

        footage_to = inner_message.get("footageTo")
        self._check_attribute_not_none(footage_to, "footageTo")

        upload_started = inner_message.get("uploadStarted")
        self._check_attribute_not_none(upload_started, "uploadStarted")

        upload_finished = inner_message.get("uploadFinished")
        self._check_attribute_not_none(upload_finished, "uploadFinished")

        tenant = self._get_tenant_id(sqs_message)
        device = self._get_device_id(sqs_message)

        yield (KinesisVideoArtifact(
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
