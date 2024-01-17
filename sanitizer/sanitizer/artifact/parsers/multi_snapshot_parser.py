""" Snapshot Parser module """
from datetime import datetime
import logging
from typing import Iterator, Optional, Union

from kink import inject

from base.aws.model import SQSMessage
from base.model.artifacts import (MultiSnapshotArtifact, RecorderType,
                                  SnapshotArtifact, TimeWindow)
from sanitizer.artifact.parsers.iparser import IArtifactParser
from sanitizer.artifact.parsers.snapshot_parser import SnapshotParser
from sanitizer.exceptions import InvalidMessageError
from sanitizer.message.message_parser import MessageParser

_logger = logging.getLogger(__name__)


@inject
class MultiSnapshotParser(IArtifactParser):  # pylint: disable=too-few-public-methods
    """SnapshotPreviewParser class"""

    def __init__(self, snapshot_parser: SnapshotParser):
        self.__snapshot_parser = snapshot_parser

    @staticmethod
    def _calculate_artifact_id(tenant_id: str, device_id: str, recording_id: str, timestamp: datetime) -> str:
        """Artifact ID for the multiple snapshots artifacts"""
        return f"{tenant_id}_{device_id}_{recording_id}_{round(timestamp.timestamp() * 1000)}"

    def parse(self, sqs_message: SQSMessage,
              recorder_type: Optional[RecorderType]) -> Iterator[Union[SnapshotArtifact, MultiSnapshotArtifact]]:
        """Generator method for extracting a list of snapshot of previews artifacts

        Args:
            message (SQSMessage): incoming SQS message

        Raises:
            InvalidMessageError: error parsing the incoming message

        Yields:
            Iterator[Artifact]: iterator of snapshot or preview artifacts
        """
        self._check_recorder_not_none(recorder_type)

        snapshots = sorted(self.__snapshot_parser.parse(sqs_message, recorder_type), key=lambda x: x.timestamp)
        for snapshot in snapshots:
            yield snapshot

        # Get the recording_id
        recording_id = MessageParser.flatten_string_value(MessageParser.get_recursive_from_dict(
            sqs_message.body,
            "Message",
            "value",
            "properties",
            "recording_id",
            default={}))

        if not snapshots:
            raise InvalidMessageError(
                "Preview Snapshot Message does not contain any chunks")  # pylint: disable=line-too-long

        if not recording_id:
            raise InvalidMessageError("Preview Recording does not contain recording id")

        first_snapshot = snapshots[0]
        last_snapshot = snapshots[-1]
        artifact_id = self._calculate_artifact_id(
            first_snapshot.tenant_id,
            first_snapshot.device_id,
            recording_id,
            first_snapshot.timestamp)
        yield MultiSnapshotArtifact(artifact_id=artifact_id,
                                    chunks=snapshots,
                                    recording_id=recording_id,
                                    tenant_id=first_snapshot.tenant_id,
                                    device_id=first_snapshot.device_id,
                                    resolution=first_snapshot.resolution,
                                    recorder=recorder_type,
                                    timestamp=first_snapshot.timestamp,
                                    end_timestamp=last_snapshot.end_timestamp,
                                    upload_timing=TimeWindow(start=first_snapshot.upload_timing.start,
                                                             end=last_snapshot.upload_timing.end)
                                    )
