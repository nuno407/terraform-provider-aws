"""Convert SQS messages into artifacts"""
import re
import logging
from typing import Callable, Union

from kink import inject
from mypy_boto3_sqs.type_defs import MessageTypeDef

from base.model.artifacts.processing_result import (AnonymizationResult,
                                                    CHCResult,
                                                    IMUProcessingResult,
                                                    PipelineProcessingStatus,
                                                    ProcessingResult,
                                                    SignalsProcessingResult,
                                                    PayloadType)

from base.aws.sqs import parse_message_body_to_dict
from artifact_downloader.config import ArtifactDownloaderConfig
from artifact_downloader.exceptions import (UnknownMDFParserArtifact,
                                            UnknownSQSMessage, UnexpectedContainerMessage)
from artifact_downloader.message.incoming_messages import MessageAttributesWithSourceContainer
_logger = logging.getLogger(__name__)


@inject
class RawMessageParser:  # pylint: disable=too-few-public-methods
    """
    Temporary class that parses raw SQS messages into artifacts

    """

    def __init__(self, config: ArtifactDownloaderConfig):
        """
        Constructor

        Args:
            config (DownloaderConfig): The config used to get the raw bucket name
        """
        self.__config = config
        self.__regex_id_from_path = r"\/([^\/]+)\."
        self.__regex_id_from_tenant = r"([^\/]+).+"

        self.__source_router: dict[str, Callable[[dict], ProcessingResult]] = {
            "anonymize": self.__handle_anon_message,
            "chc": self.__handle_chc_message,
            "sdm": self.__handle_sdm_message,
            "mdfparser": self.__handle_mdfparser_message,
        }

    def __get_parsed_message(self, message: MessageTypeDef) -> tuple[dict, str]:
        """
        Parses the json body and the source container

        Args:
            message (MessageTypeDef): The SQS message

        Raises:
            UnexpectedContainerMessage: If it cannot be parsed into a json

        Returns:
            tuple[dict, str]: A tuple containing the json message and the source container value
        """
        body = message.get("Body")
        if not body:
            raise UnexpectedContainerMessage

        json_message = parse_message_body_to_dict(body)

        message_attributes = MessageAttributesWithSourceContainer[str](
            **message.get("MessageAttributes", {}))
        source_container = message_attributes.source_container.lower()

        return json_message, source_container

    def adapt_message(self, message: MessageTypeDef) -> MessageTypeDef:
        """
        Parses a raw SQS message from anon_ivschain, chc_ivschain, sdm, mdfparser.
        The returned message is parsed and will contain the artifact in string format.

        Args:
            body (str): The message body
            source_container (str): The source component that sent the message (case insensitive)

        Raises:
            UknownSQSMessage: If the source container cannot be handled

        Returns:
            MessageTypeDef: The message with the artifact
        """
        json_message, source_container = self.__get_parsed_message(message)

        handler = self.__source_router.get(source_container)
        if handler is None:
            return message

        _logger.debug(
            "Message sent is not yet a pydantic model... Converting it")

        pydantic_model_message = handler(json_message)
        message["Body"] = pydantic_model_message.stringify()

        _logger.info(
            "Message has been converted to a pydantic model. Message=%s", message["Body"])
        return message

    def __get_id_from_path(self, path: str) -> str:
        """
        Returns the ID based in the path.
        It will attempt to parse the content bettwen the last "/" and the last "."

        Args:
            path (str): The path in to the file (can be an s3 path or not)

        Raises:
            UknownSQSMessage: Raises an exception if a match is not found

        Returns:
            str: The ID of the file
        """
        matches = re.search(self.__regex_id_from_path, path)
        if not matches:
            raise UnknownSQSMessage("Could not parse s3 raw path")

        return matches.group(1)

    def __get_tenant_from_path(self, path: str) -> str:
        """
        Returns the tenant based in the path.

        Args:
            path (str): The path in to the file (can be an s3 path or not)

        Raises:
            UnknownSQSMessage: Raises an exception if a match is not found

        Returns:
            str: The tenant of the file
        """
        matches = re.search(self.__regex_id_from_tenant, path)
        if not matches:
            raise UnknownSQSMessage("Could not parse s3 raw path")

        return matches.group(1)

    def __generate_s3_path(self, bucket: str, path: str) -> str:
        """
        Generates the s3 path based on a bucket and a path

        Args:
            bucket (str): The bucket
            path (str): The path to the file

        Returns:
            str: The s3 path
        """

        if path.startswith("/"):
            path = path[1:]
        return f"s3://{bucket}/{path}"

    def __handle_anon_message(self, body: dict) -> AnonymizationResult:
        """
        Handles messages arriving from anon ivschain

        Args:
            body (dict): The message body

        Returns:
            AnonymizationResult: The result artifact
        """
        bucket = body["output"]["bucket"]
        anon_video_path = body["output"]["media_path"]
        raw_video_path = body["s3_path"]

        return AnonymizationResult(
            correlation_id=self.__get_id_from_path(raw_video_path),
            raw_s3_path=self.__generate_s3_path(
                self.__config.raw_bucket, raw_video_path),
            s3_path=self.__generate_s3_path(bucket, anon_video_path),
            tenant_id=self.__get_tenant_from_path(raw_video_path),
            processing_status=body["data_status"]
        )

    def __handle_chc_message(self, body: dict) -> CHCResult:
        """
        Handles messages arriving from CHC

        Args:
            body (dict): The message body

        Returns:
            CHCResult: The result artifact
        """
        bucket = body["output"]["bucket"]
        chc_path = body["output"]["meta_path"]
        raw_video_path = body["s3_path"]

        return CHCResult(
            correlation_id=self.__get_id_from_path(raw_video_path),
            raw_s3_path=self.__generate_s3_path(
                self.__config.raw_bucket, raw_video_path),
            tenant_id=self.__get_tenant_from_path(raw_video_path),
            s3_path=self.__generate_s3_path(bucket, chc_path),
            processing_status=body["data_status"]
        )

    def __handle_sdm_message(self, body: dict) -> PipelineProcessingStatus:
        """
        Handles messages arriving from SDM

        Args:
            body (dict): The message body

        Returns:
            PipelineProcessingStatus: The result artifact
        """
        raw_video_path = body["s3_path"]

        payload_type = PayloadType.VIDEO if raw_video_path.endswith(
            ".mp4") else PayloadType.SNAPSHOT

        return PipelineProcessingStatus(
            correlation_id=self.__get_id_from_path(raw_video_path),
            s3_path=self.__generate_s3_path(
                self.__config.raw_bucket, raw_video_path),
            tenant_id=self.__get_tenant_from_path(raw_video_path),
            info_source="SDM",
            object_type=payload_type,
            processing_status=body["data_status"],
            processing_steps=body["processing_steps"]
        )

    def __handle_mdfparser_message(
            self, body: dict) -> Union[IMUProcessingResult, SignalsProcessingResult]:
        """
        Handles messages arriving from mdfparser

        Args:
            body (dict): The message body

        Returns:
            Union[IMUProcessingResult, SignalsProcessingResult]: The result artifact
        """

        data_type = body["data_type"]
        if data_type == "imu":
            return IMUProcessingResult(
                correlation_id=body["_id"],
                s3_path=body["parsed_file_path"],
                tenant_id=body["tenant"],
                video_raw_s3_path=body["raw_s3_path"],
            )
        if data_type == "metadata":
            return SignalsProcessingResult(
                correlation_id=body["_id"],
                s3_path=body["parsed_file_path"],
                tenant_id=body["tenant"],
                recording_overview=body["recording_overview"],
                video_raw_s3_path=body["raw_s3_path"]
            )
        raise UnknownMDFParserArtifact(
            f"Uknown mdfparser artifact data_type={data_type}")
