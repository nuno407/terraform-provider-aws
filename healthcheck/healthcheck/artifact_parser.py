"""Artifact parser module."""
from datetime import datetime
from logging import Logger
from typing import Dict, Iterator, Optional

from kink import inject

from healthcheck.exceptions import InvalidMessageCanSkip, InvalidMessageError
from healthcheck.model import (Artifact, SnapshotArtifact, SQSMessage,
                               VideoArtifact)

VIDEO = ["InteriorRecorder", "TrainingRecorder", "FrontRecorder"]
IMAGE = ["TrainingMultiSnapshot"]


@inject
class ArtifactParser:
    """Artifact Message Parser"""

    def __init__(self, logger: Logger) -> None:
        self.__logger = logger

    @staticmethod
    def get_recursive_from_dict(  # pylint: disable=dangerous-default-value
            data_dict: dict,
            *keys: str,
            default=None):
        """Get value from dict recursively."""
        if default is None:
            default = []
        for key in keys:
            if not isinstance(data_dict, Dict) or key not in data_dict:
                return default
            data_dict = data_dict[key]
        return data_dict

    @staticmethod
    def __contains_identifier(raw_body: str, identifier: str) -> bool:
        return raw_body.find(identifier) != -1

    @staticmethod
    def __contains_any_identifier(raw_body: str, identifiers: list[str]) -> bool:
        return any(ArtifactParser.__contains_identifier(raw_body, identifier) for identifier in identifiers)

    @staticmethod
    def message_type_identifier(message: SQSMessage) -> Optional[str]:
        """ Identify if the type of the media described in the message.
            Only returns a type when its sure its from that type otherwise returns None.

        Args:
            message (SQSMessage): message to identify

        Returns:
            result (Optional[str]): type identified, defaults to None.
        """
        result = None
        raw_body = str(message.body)

        recorders = ["InteriorRecorder", "TrainingRecorder", "TrainingMultiSnapshot", "FrontRecorder"]
        for recorder in recorders:
            other_recorders = [r for r in recorders if r != recorder]
            if (ArtifactParser.__contains_identifier(raw_body, recorder) and not
                    ArtifactParser.__contains_any_identifier(raw_body, other_recorders)):
                result = recorder
                break

        return result

    def parse_message(self, message: SQSMessage) -> list[Artifact]:
        """Extracts artifacts from the message

        Args:
            message (SQSMessage): incoming SQS message (input or footage event)

        Returns:
            list[Artifact]: list of artifacts
        """
        self.__logger.debug("parsing sqs message into artifact")
        if "TopicArn" in message.body:
            self.__logger.debug(
                "incoming message from topic %s", message.body["TopicArn"])

        if self.message_type_identifier(message) in IMAGE:  # pylint: disable=no-else-return
            return list(self.__extract_snapshots(message))
        elif self.message_type_identifier(message) in VIDEO:
            return [self.__extract_recording(message)]
        else:
            raise InvalidMessageError("Unknown message type")

    def __extract_snapshots(self, message: SQSMessage) -> Iterator[Artifact]:
        """Generator method for extracting a list of snapshot artifacts

        Args:
            message (SQSMessage): incoming SQS message

        Raises:
            InvalidMessageError: error parsing the incoming message

        Yields:
            Iterator[Artifact]: iterator of snapshot artifacts
        """
        # get tenant information
        self.__logger.debug(
            "starting extracting snapshot artifact from sqs message...")

        self.__logger.debug("getting snapshot tenant...")
        tenant_property = ArtifactParser.get_recursive_from_dict(
            message.body,
            "MessageAttributes",
            "tenant")
        if "Value" in tenant_property:
            tenant = tenant_property["Value"]
        elif "StringValue" in tenant_property:
            tenant = tenant_property["StringValue"]
        else:
            raise InvalidMessageError(
                "Invalid message body. Cannot extract tenant.")

        # get device id
        self.__logger.debug("getting snapshot device_id...")
        device_id = ArtifactParser.get_recursive_from_dict(
            message.body, "Message", "value", "properties", "header", "device_id")
        if not isinstance(device_id, str):
            raise InvalidMessageError(
                "Invalid message body. Cannot extract device_id.")

        # get chunks
        self.__logger.debug("getting snapshot chunk_descriptions...")
        chunks = ArtifactParser.get_recursive_from_dict(
            message.body,
            "Message",
            "value",
            "properties",
            "chunk_descriptions")
        if len(chunks) == 0:
            raise InvalidMessageCanSkip(
                "Invalid message body. Cannot extract snapshots.")

        # extract snapshots from chunks
        self.__logger.debug("extracting snapshots from all chunks...")
        for chunk in chunks:
            if not ("uuid" in chunk and "start_timestamp_ms" in chunk):
                raise InvalidMessageError(
                    "Invalid snapshot chunk. Missing uuid or start_timestamp_ms.")  # pylint: disable=line-too-long
            yield SnapshotArtifact(tenant_id=tenant,
                                   device_id=device_id,
                                   uuid=chunk["uuid"],
                                   timestamp=datetime.fromtimestamp(chunk["start_timestamp_ms"] / 1000.0))  # pylint: disable=line-too-long

    def __extract_recording(self, message: SQSMessage) -> Artifact:
        """Extract recording artifact from SQS message

        Args:
            message (SQSMessage): incoming SQS message

        Raises:
            InvalidMessageError: error parsing the incoming message

        Returns:
            Artifact: Recording artifact
        """
        self.__logger.debug(
            "starting extracting video artifact from sqs message...")
        if not message.body:
            raise InvalidMessageError("Invalid message, empty body.")

        self.__logger.debug("extracting video inner message body...")
        inner_message: Optional[dict] = message.body.get("Message")
        if not inner_message:
            raise InvalidMessageError(
                "Invalid message body. Cannot extract message contents.")

        self.__logger.debug("extracting video stream name...")
        stream_name = inner_message.get("streamName")
        if not stream_name:
            raise InvalidMessageError(
                "Invalid message body. Cannot extract stream name.")

        self.__logger.debug("extracting video footageFrom...")
        footage_from = inner_message.get("footageFrom")
        if not footage_from:
            raise InvalidMessageError(
                "Invalid message body. Cannot extract footageFrom.")

        self.__logger.debug("extracting video footageTo...")
        footage_to = inner_message.get("footageTo")
        if not footage_to:
            raise InvalidMessageError(
                "Invalid message body. Cannot extract footageTo.")

        self.__logger.debug(
            "checking if video message attribute tenant is present...")
        if not message.attributes.tenant:
            raise InvalidMessageError(
                "Invalid message attribute. Cannot extract tenant.")

        self.__logger.debug(
            "checking if video attribute device_id is present...")
        if not message.attributes.device_id:
            raise InvalidMessageError(
                "Invalid message attribute. Cannot extract deviceId.")

        return VideoArtifact(
            tenant_id=message.attributes.tenant,
            device_id=message.attributes.device_id,
            stream_name=stream_name,
            footage_from=datetime.fromtimestamp(footage_from / 1000.0),
            footage_to=datetime.fromtimestamp(footage_to / 1000.0))
