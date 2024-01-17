# pylint: disable=duplicate-code
""" Snapshot Parser module """
import logging
from datetime import timedelta, datetime
from typing import Iterator, Optional
from pathlib import Path
from kink import inject

from base.aws.model import SQSMessage
from base.model.artifacts import RecorderType, SnapshotArtifact, TimeWindow
from base.timestamps import from_epoch_seconds_or_milliseconds
from sanitizer.artifact.parsers.iparser import IArtifactParser
from sanitizer.config import SanitizerConfig
from sanitizer.exceptions import ArtifactException, InvalidMessageError
from sanitizer.message.message_parser import MessageParser
from sanitizer.artifact.parsers.utils import calculate_anonymized_s3_path, calculate_raw_s3_path

_logger = logging.getLogger(__name__)


@inject
class SnapshotParser(IArtifactParser):  # pylint: disable=too-few-public-methods,duplicate-code
    """SnapshotParser class"""

    def __init__(self, sanitizer_config: SanitizerConfig):
        self.__sanitizer_config = sanitizer_config

    @staticmethod
    def _calculate_artifact_id(tenant_id: str, device_id: str, uuid: str, timestamp: datetime):
        """ Statically calculates artifact id. """
        uuid_without_ext = uuid.removesuffix(Path(uuid).suffix)
        return f"{tenant_id}_{device_id}_{uuid_without_ext}_{round(timestamp.timestamp() * 1000)}"

    # pylint: disable=too-many-locals
    def parse(self, sqs_message: SQSMessage, recorder_type: Optional[RecorderType]) -> Iterator[SnapshotArtifact]:
        """Generator method for extracting a list of snapshot or previews artifacts

        Args:
            message (SQSMessage): incoming SQS message

        Raises:
            InvalidMessageError: error parsing the incoming message

        Yields:
            Iterator[Artifact]: iterator of snapshot or preview artifacts
        """
        self._check_recorder_not_none(recorder_type)
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
        upload_timing = TimeWindow(start=upload_finished, end=upload_finished)  # type: ignore
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

                # It is correct that we do not have a path for the uploaded snapshot? :o
                # How can we check what format is the file?
                file_extension = "jpeg"
                # rcc_path.split(".")[-1].lower()
                # if file_extension not in VIDEO_FORMATS:
                #    raise InvalidMessageError("File extension not compatible. Extension %s", file_extension)
                artifact_id = self._calculate_artifact_id(
                    tenant_id=tenant,
                    device_id=device_id,
                    uuid=chunk["uuid"],
                    timestamp=from_epoch_seconds_or_milliseconds(
                        chunk["start_timestamp_ms"]))
                devcloud_raw_filepath = calculate_raw_s3_path(
                    s3_bucket=self.__sanitizer_config.devcloud_raw_bucket,
                    tenant_id=tenant,
                    artifact_id=artifact_id,
                    file_extension=file_extension)
                devcloud_anonymized_filepath = calculate_anonymized_s3_path(
                    s3_bucket=self.__sanitizer_config.devcloud_anonymized_bucket,
                    tenant_id=tenant,
                    artifact_id=artifact_id,
                    file_extension=file_extension)
                yield SnapshotArtifact(artifact_id=artifact_id,
                                       tenant_id=tenant,
                                       device_id=device_id,
                                       s3_path=devcloud_raw_filepath,
                                       raw_s3_path=devcloud_raw_filepath,
                                       anonymized_s3_path=devcloud_anonymized_filepath,
                                       uuid=chunk["uuid"],
                                       recorder=recorder_type,
                                       timestamp=from_epoch_seconds_or_milliseconds(chunk["start_timestamp_ms"]),
                                       end_timestamp=end_timestamp,
                                       upload_timing=upload_timing)  # pylint: disable=line-too-long
            except ArtifactException as err:
                _logger.exception("Error parsing snapshot artifact: %s", err)
