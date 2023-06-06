""" Snapshot Parser module """
import logging
from datetime import timedelta
from typing import Iterator

from kink import inject

from base.aws.model import SQSMessage
from base.model.artifacts import RecorderType, SnapshotArtifact, TimeWindow
from base.timestamps import from_epoch_seconds_or_milliseconds
from sanitizer.artifact.parsers.iparser import IArtifactParser
from sanitizer.exceptions import ArtifactException, InvalidMessageError
from sanitizer.message.message_parser import MessageParser

_logger = logging.getLogger(__name__)


@inject
class SnapshotParser(IArtifactParser):  # pylint: disable=too-few-public-methods
    """SnapshotParser class"""

    def parse(self, sqs_message: SQSMessage, recorder_type: RecorderType) -> Iterator[SnapshotArtifact]:
        """Generator method for extracting a list of snapshot or previews artifacts

        Args:
            message (SQSMessage): incoming SQS message

        Raises:
            InvalidMessageError: error parsing the incoming message

        Yields:
            Iterator[Artifact]: iterator of snapshot or preview artifacts
        """
        # get tenant information
        tenant = MessageParser.flatten_string_value(MessageParser.get_recursive_from_dict(
            sqs_message.body,
            "MessageAttributes",
            "tenant",
            default={}))
        if tenant is None:
            raise InvalidMessageError(
                "Invalid message body. Cannot extract tenant.")

        # get device id
        device_id = MessageParser.flatten_string_value(MessageParser.get_recursive_from_dict(
            sqs_message.body,
            "Message",
            "value",
            "properties",
            "header",
            "device_id",
            default={}))
        if device_id is None:
            raise InvalidMessageError(
                "Invalid message body. Cannot extract device_id.")

        # get upload finished timestamp
        upload_finished = MessageParser.flatten_string_value(MessageParser.get_recursive_from_dict(
            sqs_message.body,
            "Message",
            "timestamp",
            default=None))
        if upload_finished is None:
            raise InvalidMessageError(
                "Invalid message body. Cannot extract upload_finished timestamp.")
        upload_timing = TimeWindow(upload_finished, upload_finished)  # type: ignore
        upload_timing.start -= timedelta(hours=1)

        # get chunks
        chunks = MessageParser.get_recursive_from_dict(
            sqs_message.body,
            "Message",
            "value",
            "properties",
            "chunk_descriptions",
            default=[])
        if len(chunks) == 0:
            raise InvalidMessageError(
                "Snapshot Message does not contain any chunks. Cannot extract snapshots.")  # pylint: disable=line-too-long

        # extract snapshots from chunks
        _logger.debug("extracting snapshots from all chunks...")

        for chunk in chunks:
            try:
                if not ("uuid" in chunk and "start_timestamp_ms" in chunk):
                    raise ArtifactException(
                        "Invalid snapshot chunk. Missing uuid or start_timestamp_ms.")  # pylint: disable=line-too-long
                start_timestamp: int = chunk["start_timestamp_ms"]
                end_timestamp: int = chunk.get("end_timestamp_ms", start_timestamp)
                yield SnapshotArtifact(tenant_id=tenant,
                                       device_id=device_id,
                                       uuid=chunk["uuid"],
                                       recorder=recorder_type,
                                       timestamp=from_epoch_seconds_or_milliseconds(chunk["start_timestamp_ms"]),
                                       end_timestamp=end_timestamp,
                                       upload_timing=upload_timing)  # pylint: disable=line-too-long
            except ArtifactException as err:
                _logger.exception("Error parsing snapshot artifact: %s", err)
