""" Message handler module """
from mypy_boto3_sqs.type_defs import MessageTypeDef
from requests import Request


class MessageHandler:  # pylint: disable=too-few-public-methods
    """ Coordinates the parsing, transformation and enrichment of messages to REST requests """

    def handle(self, message: MessageTypeDef) -> Request:  # type: ignore
        """ Central entrypoint to the MessageHandler """
