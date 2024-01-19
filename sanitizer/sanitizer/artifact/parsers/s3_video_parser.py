# pylint: disable=duplicate-code
"""Module to parse videos that RCC has concatenated and uploaded to their S3."""
from datetime import datetime
from typing import Any, Iterator, Optional

from pydantic import BaseModel, Field, ValidationError
from kink import inject

from base.constants import VIDEO_FORMATS
from base.aws.model import SQSMessage
from base.model.artifacts import (RecorderType, Recording, S3VideoArtifact,
                                  TimeWindow)
from sanitizer.artifact.parsers.video_parser import VideoParser
from sanitizer.config import SanitizerConfig
from sanitizer.exceptions import InvalidMessageError
from sanitizer.artifact.parsers.utils import calculate_anonymized_s3_path, calculate_raw_s3_path


class _UploadInfo(BaseModel):
    chunks: list[Any] = Field(default_factory=list)
    recording_id: Optional[str] = Field(default=None, alias="recordingId")
    upload_started: datetime = Field(default=..., alias="uploadStarted")
    upload_finished: datetime = Field(default=..., alias="uploadFinished")


class _S3VideoInnerMessage(BaseModel):
    upload_infos: list[_UploadInfo] = Field(default_factory=list, alias="uploadInfos")
    footage_from: datetime = Field(default=..., alias="footageFrom")
    footage_to: datetime = Field(default=..., alias="footageTo")
    footage_id: str = Field(default=..., alias="footageId")
    download_link: str = Field(default=..., alias="downloadLink")


@inject
class S3VideoParser(VideoParser):  # pylint: disable=too-few-public-methods
    """Class to parse videos that RCC has concatenated and uploaded to their S3."""

    def __init__(self, sanitizer_config: SanitizerConfig):
        self.__sanitizer_config = sanitizer_config

    @staticmethod
    def _calculate_artifact_id(
            device_id: str,
            recorder: str,
            footage_id: str,
            start_timestamp: datetime,
            end_timestamp: datetime):
        """ Statically calculates artifact id"""
        return f"{device_id}_{recorder}_{footage_id}_{round(start_timestamp.timestamp() *1000)}_{round(end_timestamp.timestamp() *1000)}"  # pylint: disable=line-too-long

    # pylint: disable=too-many-locals
    def parse(self, sqs_message: SQSMessage, recorder_type: Optional[RecorderType]) -> Iterator[S3VideoArtifact]:
        """Extract recording artifact stored on S3 from SQS message

        Args:
            message (SQSMessage): incoming SQS message
            recorder_type (RecorderType): recorder type

        Raises:
            InvalidMessageError: error parsing the incoming message

        Returns:
            Artifact: Recording artifact
        """
        self._check_recorder_not_none(recorder_type)
        inner_message: dict = self._get_inner_message(sqs_message)
        try:
            parsed_message = _S3VideoInnerMessage.model_validate(inner_message)
        except ValidationError as err:
            raise InvalidMessageError(
                "Invalid message body. Cannot extract message contents.") from err

        if len(parsed_message.upload_infos) == 0:
            raise InvalidMessageError(
                "Invalid message body. Not enough uploadInfos in message.")

        upload_start = min(upload_info.upload_started for upload_info in parsed_message.upload_infos)
        upload_end = max(upload_info.upload_finished for upload_info in parsed_message.upload_infos)

        recordings = [Recording(recording_id=upload_info.recording_id, chunk_ids=upload_info.chunks) for upload_info in
                      parsed_message.upload_infos if upload_info.recording_id]

        tenant = self._get_tenant_id(sqs_message)
        device = self._get_device_id(sqs_message)

        rcc_path = parsed_message.download_link
        if rcc_path.endswith("_watermark.mp4"):
            rcc_path = rcc_path[:-len("_watermark.mp4")] + ".mp4"

        file_extension = rcc_path.split(".")[-1].lower()
        if file_extension not in VIDEO_FORMATS:
            raise InvalidMessageError(f"File extension not compatible. Extension {file_extension}")

        artifact_id = self._calculate_artifact_id(device_id=device,
                                                  recorder=recorder_type.value,  # type: ignore
                                                  footage_id=parsed_message.footage_id,
                                                  start_timestamp=parsed_message.footage_from,
                                                  end_timestamp=parsed_message.footage_to)
        devcloud_raw_filepath = calculate_raw_s3_path(s3_bucket=self.__sanitizer_config.devcloud_raw_bucket,
                                                      tenant_id=tenant,
                                                      artifact_id=artifact_id,
                                                      file_extension=file_extension)
        # pylint: disable=line-too-long
        devcloud_anonymized_filepath = calculate_anonymized_s3_path(
            s3_bucket=self.__sanitizer_config.devcloud_anonymized_bucket,
            tenant_id=tenant,
            artifact_id=artifact_id,
            file_extension=file_extension)
        yield S3VideoArtifact(
            artifact_id=artifact_id,
            tenant_id=tenant,
            s3_path=devcloud_raw_filepath,
            raw_s3_path=devcloud_raw_filepath,
            anonymized_s3_path=devcloud_anonymized_filepath,
            device_id=device,
            recorder=recorder_type,
            timestamp=parsed_message.footage_from,
            end_timestamp=parsed_message.footage_to,
            footage_id=parsed_message.footage_id,
            upload_timing=TimeWindow(start=upload_start, end=upload_end),
            rcc_s3_path=rcc_path,
            recordings=recordings
        )
