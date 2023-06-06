"""Module to parse videos that RCC has concatenated and uploaded to their S3."""
from datetime import datetime
from typing import Any, Iterator, Optional

from kink import inject
from pydantic import BaseModel, Field, ValidationError

from base.aws.model import SQSMessage
from base.model.artifacts import RecorderType, S3VideoArtifact, TimeWindow
from sanitizer.artifact.parsers.video_parser import VideoParser
from sanitizer.exceptions import InvalidMessageError


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

    def parse(self, sqs_message: SQSMessage, recorder_type: RecorderType) -> Iterator[S3VideoArtifact]:
        """Extract recording artifact stored on S3 from SQS message

        Args:
            message (SQSMessage): incoming SQS message
            recorder_type (RecorderType): recorder type

        Raises:
            InvalidMessageError: error parsing the incoming message

        Returns:
            Artifact: Recording artifact
        """
        inner_message: dict = self._get_inner_message(sqs_message)
        try:
            parsed_message = _S3VideoInnerMessage.parse_obj(inner_message)
        except ValidationError as err:
            raise InvalidMessageError(
                "Invalid message body. Cannot extract message contents.") from err

        if len(parsed_message.upload_infos) == 0:
            raise InvalidMessageError(
                "Invalid message body. Not enough uploadInfos in message.")

        upload_start = min(upload_info.upload_started for upload_info in parsed_message.upload_infos)
        upload_end = max(upload_info.upload_finished for upload_info in parsed_message.upload_infos)

        tenant = self._get_tenant_id(sqs_message)
        device = self._get_device_id(sqs_message)

        rcc_path = parsed_message.download_link
        if rcc_path.endswith("_watermark.mp4"):
            rcc_path = rcc_path[:-len("_watermark.mp4")] + ".mp4"

        yield S3VideoArtifact(
            tenant_id=tenant,
            device_id=device,
            recorder=recorder_type,
            timestamp=parsed_message.footage_from,
            end_timestamp=parsed_message.footage_to,
            footage_id=parsed_message.footage_id,
            upload_timing=TimeWindow(start=upload_start, end=upload_end),
            rcc_s3_path=rcc_path
        )
