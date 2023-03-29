""" Snapshot Parser module """
import logging
from typing import Iterator

from kink import inject

from base.aws.model import SQSMessage
from base.model.artifacts import RecorderType, SnapshotArtifact
from base.timestamps import from_epoch_seconds_or_milliseconds
from sanitizer.exceptions import ArtifactException, InvalidMessageError
from sanitizer.message.message_parser import MessageParser

__logger = logging.getLogger(__name__)

@inject
class SnapshotParser: # pylint: disable=too-few-public-methods
    """SnapshotParser class"""

    def parse(self, sqs_message: SQSMessage) -> Iterator[SnapshotArtifact]:
        """Generator method for extracting a list of snapshot artifacts

        Args:
            message (SQSMessage): incoming SQS message

        Raises:
            InvalidMessageError: error parsing the incoming message

        Yields:
            Iterator[Artifact]: iterator of snapshot artifacts
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
        __logger.debug("extracting snapshots from all chunks...")

        for chunk in chunks:
            try:
                if not ("uuid" in chunk and "start_timestamp_ms" in chunk):
                    raise ArtifactException(
                        "Invalid snapshot chunk. Missing uuid or start_timestamp_ms.")  # pylint: disable=line-too-long
                yield SnapshotArtifact(tenant_id=tenant,
                                       device_id=device_id,
                                       uuid=chunk["uuid"],
                                       recorder=RecorderType.SNAPSHOT,
                                       timestamp=from_epoch_seconds_or_milliseconds(chunk["start_timestamp_ms"]))  # pylint: disable=line-too-long
            except ArtifactException as err:
                __logger.exception("Error parsing snapshot artifact: %s", err)
