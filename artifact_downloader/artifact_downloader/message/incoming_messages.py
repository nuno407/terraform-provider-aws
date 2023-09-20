import json
from ast import literal_eval
from typing import Generic, Literal, TypeVar

from pydantic import Field, validator

from base.model.artifacts import DiscriminatedArtifact
from base.model.base_model import ConfiguredGenericModel

BodyType = TypeVar("BodyType", bound=ConfiguredGenericModel)
AttributeType = TypeVar("AttributeType", bound=ConfiguredGenericModel)
SourceContainerLiteral = TypeVar("SourceContainerLiteral")


class MessageAttributesWithSourceContainer(ConfiguredGenericModel, Generic[SourceContainerLiteral]):
    source_container: SourceContainerLiteral = Field(alias="SourceContainer")

    @validator("source_container", pre=True)
    def parse_source_container(val) -> str:
        if isinstance(val, str):
            return val
        if isinstance(val, dict):
            if "Value" in val and isinstance(val["Value"], str):
                return val["Value"]
            if "StringValue" in val and isinstance(val["StringValue"], str):
                return val["StringValue"]
        raise ValueError("Could not parse source container from message")


class SqsMessage(ConfiguredGenericModel, Generic[BodyType, AttributeType]):
    message_id: str = Field(alias="MessageId")
    body: BodyType = Field(alias="Body")
    receipt_handle: str = Field(alias="ReceiptHandle")
    attributes: AttributeType = Field(alias="MessageAttributes")

    @validator("body", pre=True)
    def parse_body(body) -> dict:
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
            data["Message"] = SqsMessage.parse_body(data["Message"])
        return data


class SanitizerMessage(SqsMessage[DiscriminatedArtifact, MessageAttributesWithSourceContainer[Literal["Sanitizer"]]]):
    pass
