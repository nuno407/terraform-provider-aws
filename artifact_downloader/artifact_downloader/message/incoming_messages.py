# pylint: disable=too-few-public-methods
""" SQS Message parsing """
import json
from ast import literal_eval
from typing import Generic, Literal, TypeVar, Union
from mypy_boto3_sqs.type_defs import MessageTypeDef
from pydantic import Field, field_validator, TypeAdapter

from base.model.artifacts import AnnotatedArtifacts
from base.model.base_model import ConfiguredBaseModel


BodyType = TypeVar("BodyType", bound=ConfiguredBaseModel)  # pylint: disable=invalid-name
AttributeType = TypeVar("AttributeType", bound=ConfiguredBaseModel)  # pylint: disable=invalid-name
SourceContainerLiteral = TypeVar("SourceContainerLiteral")


class MessageAttributesWithSourceContainer(ConfiguredBaseModel, Generic[SourceContainerLiteral]):
    """MessageAtributes pydantic model for SQS message attributes"""
    source_container: SourceContainerLiteral = Field(alias="SourceContainer")

    @field_validator("source_container", mode="before")
    def parse_source_container(cls, val) -> str:
        """
        Parses the source container
        """
        if isinstance(val, str):
            return val
        if isinstance(val, dict):
            if "Value" in val and isinstance(val["Value"], str):
                return val["Value"]
            if "StringValue" in val and isinstance(val["StringValue"], str):
                return val["StringValue"]
            if "stringValue" in val and isinstance(val["stringValue"], str):
                return val["stringValue"]
        raise ValueError("Could not parse source container from message")


class SqsMessage(ConfiguredBaseModel, Generic[BodyType, AttributeType]):
    """
    Pydantic model for an SQS message
    """
    message_id: str = Field(alias="MessageId")
    body: BodyType = Field(alias="Body")
    receipt_handle: str = Field(alias="ReceiptHandle")
    attributes: AttributeType = Field(alias="MessageAttributes")

    @field_validator("body", mode="before")
    def parse_body(cls, body) -> dict:
        """Parses the body of an SQS message"""
        if isinstance(body, dict):
            return body
        try:
            data = json.loads(body)
        except json.JSONDecodeError as json_exc:
            try:
                data = literal_eval(body)
            except (ValueError, TypeError, SyntaxError, MemoryError, RecursionError) as eval_exc:
                raise ValueError(
                    "Unable to parse message content from JSON or python object", [
                        json_exc, eval_exc]) from eval_exc

        if "Message" in data and isinstance(data["Message"], str):
            data["Message"] = SqsMessage.parse_body(data["Message"])  # type: ignore
        return data


class SanitizerMessage(SqsMessage[AnnotatedArtifacts, MessageAttributesWithSourceContainer[Literal["Sanitizer"]]]):
    """ SanitizerMessage """


class SDRetrieverMessage(SqsMessage[AnnotatedArtifacts,
                                    MessageAttributesWithSourceContainer[Literal["SDRetriever"]]]):
    """ SDRMessage """


class MDFParserMessage(SqsMessage[AnnotatedArtifacts, MessageAttributesWithSourceContainer[Literal["MDFParser"]]]):
    """ MDFParserMessage """


class AnonymizeMessage(SqsMessage[AnnotatedArtifacts, MessageAttributesWithSourceContainer[Literal["Anonymize"]]]):
    """ AnonymizeMessage """


class CHCMessage(SqsMessage[AnnotatedArtifacts, MessageAttributesWithSourceContainer[Literal["CHC"]]]):
    """ CHCMessage """


class SDMMessage(SqsMessage[AnnotatedArtifacts, MessageAttributesWithSourceContainer[Literal["SDM"]]]):
    """ SDMMessage """


SQSMessageAdapter = TypeAdapter(Union[SDMMessage, CHCMessage, AnonymizeMessage,
                                MDFParserMessage, SDRetrieverMessage, SanitizerMessage])


def parse_sqs_message(message: MessageTypeDef) -> SqsMessage:
    """
    Parses an SQS message

    Args:
        message (MessageTypeDef): The raw SQS message

    Returns:
        SqsMessage: The respective container message
    """
    return SQSMessageAdapter.validate_python(message)  # type: ignore
