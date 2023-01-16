"""Artifact parser module."""
from datetime import datetime
from typing import Dict, Iterator

from healthcheck.exceptions import InvalidMessageError
from healthcheck.model import Artifact, SQSMessage, SnapshotArtifact, VideoArtifact, InvalidMessageCanSkip


class ArtifactParser():
    """Artifact Message Parser"""

    @staticmethod
    def get_recursive_from_dict(data_dict: dict, *keys: str, default=[]):
        """Get value from dict recursively."""
        for key in keys:
            if not isinstance(data_dict, Dict) or key not in data_dict:
                return default
            data_dict = data_dict[key]
        return data_dict

    def parse_message(self, message: SQSMessage) -> list[Artifact]:
        """Extracts artifacts from the message

        Args:
            message (SQSMessage): incoming SQS message (input or footage event)

        Returns:
            list[Artifact]: list of artifacts
        """
        if "TopicArn" not in message.body:
            raise InvalidMessageError("TopicArn missing in message body. Cannot determine message type.")
        if message.body["TopicArn"].endswith("inputEventsTerraform"):
            # snapshot message
            return list(self.__extract_snapshots(message))
        if message.body["TopicArn"].endswith("video-footage-events"):
            # footage message
            return [self.__extract_recording(message)]

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
        tenant_property = ArtifactParser.get_recursive_from_dict(message.body, "MessageAttributes", "tenant")
        if "Value" in tenant_property:
            tenant = tenant_property["Value"]
        elif "StringValue" in tenant_property:
            tenant = tenant_property["StringValue"]
        else:
            raise InvalidMessageError("Invalid message body. Cannot extract tenant.")

        # get device id
        device_id = ArtifactParser.get_recursive_from_dict(message.body, "Message", "value", "properties", "header", "device_id")
        if not isinstance(device_id, str):
            raise InvalidMessageError("Invalid message body. Cannot extract device_id.")

        # get chunks
        chunks = ArtifactParser.get_recursive_from_dict(
            message.body, "Message", "value", "properties", "chunk_descriptions")
        if len(chunks) == 0 != str:
            raise InvalidMessageCanSkip("Invalid message body. Cannot extract snapshots.")

        # extract snapshots from chunks
        for chunk in chunks:
            if not ("uuid" in chunk and "start_timestamp_ms" in chunk):
                raise InvalidMessageError("Invalid snapshot chunk. Missing uuid or start_timestamp_ms.")
            yield SnapshotArtifact(tenant_id=tenant,
                                   device_id=device_id,
                                   uuid=chunk["uuid"],
                                   timestamp=datetime.fromtimestamp(chunk["start_timestamp_ms"] / 1000.0))

    def __extract_recording(self, message: SQSMessage) -> Artifact:
        """Extract recording artifact from SQS message

        Args:
            message (SQSMessage): incoming SQS message

        Raises:
            InvalidMessageError: error parsing the incoming message

        Returns:
            Artifact: Recording artifact
        """
        if not message.body:
            raise InvalidMessageError("Invalid message, empty body.")

        inner_message = message.body.get("Message")
        if not inner_message:
            raise InvalidMessageError("Invalid message body. Cannot extract inner message contents.")

        stream_name = inner_message.get("streamName")
        if not stream_name:
            raise InvalidMessageError("Invalid message body. Cannot extract stream name.")

        footage_from = inner_message.get("footageFrom")
        if not footage_from:
            raise InvalidMessageError("Invalid message body. Cannot extract footageFrom.")

        footage_to = inner_message.get("footageTo")
        if not footage_to:
            raise InvalidMessageError("Invalid message body. Cannot extract footageTo.")

        if not message.attributes.tenant:
            raise InvalidMessageError("Invalid message attribute. Cannot extract tenant.")

        if not message.attributes.device_id:
            raise InvalidMessageError("Invalid message attribute. Cannot extract deviceId.")

        return VideoArtifact(
            tenant_id=message.attributes.tenant,
            device_id=message.attributes.device_id,
            stream_name=stream_name,
            footage_from=datetime.fromtimestamp(footage_from / 1000.0),
            footage_to=datetime.fromtimestamp(footage_to / 1000.0))
