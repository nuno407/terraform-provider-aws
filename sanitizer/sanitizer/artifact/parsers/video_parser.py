"""Base module for video based artifact parsers"""
import json

from base.aws.model import SQSMessage
from sanitizer.artifact.parsers.iparser import IArtifactParser


class VideoParser(IArtifactParser):  # pylint: disable=too-few-public-methods
    """Base class for video based artifact parsers"""

    def _get_inner_message(self, sqs_message: SQSMessage) -> dict:
        self._check_attribute_not_none(sqs_message.body, "message body")

        inner_message: dict = sqs_message.body.get("Message")
        self._check_attribute_not_none(inner_message, "inner message")

        if isinstance(inner_message, str):
            inner_message = json.loads(inner_message)
            self._check_attribute_not_none(inner_message, "JSON parsed inner message")

        return inner_message

    def _get_tenant_id(self, sqs_message: SQSMessage) -> str:
        tenant = sqs_message.attributes.tenant
        self._check_attribute_not_none(tenant, "tenant ID")
        return tenant

    def _get_device_id(self, sqs_message: SQSMessage) -> str:
        device = sqs_message.attributes.device_id
        self._check_attribute_not_none(device, "device ID")
        return device
