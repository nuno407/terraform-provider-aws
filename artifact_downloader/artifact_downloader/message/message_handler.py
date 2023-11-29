""" Message handler module """
import logging
from typing import Type

from kink import inject
from mypy_boto3_sqs.type_defs import MessageTypeDef
from requests import Request
from base.aws.sqs import parse_message_body_to_dict

from artifact_downloader.container_handlers import (AnonymizeContainerHandler,
                                                    CHCContainerHandler,
                                                    MDFParserContainerHandler,
                                                    SanitizerContainerHandler,
                                                    SDMContainerHandler,
                                                    SDRContainerHandler)
from artifact_downloader.container_handlers.handler import ContainerHandler
from artifact_downloader.exceptions import UnknownSQSMessage
from artifact_downloader.message.incoming_messages import (AnonymizeMessage,
                                                           CHCMessage,
                                                           MDFParserMessage,
                                                           SanitizerMessage,
                                                           SDMMessage,
                                                           SDRetrieverMessage,
                                                           SqsMessage,
                                                           parse_sqs_message)
from artifact_downloader.message.raw_message_parser import RawMessageParser

_logger = logging.getLogger(__name__)


@inject
class MessageHandler:  # pylint: disable=too-few-public-methods
    """ Coordinates the parsing, transformation and enrichment of messages to REST requests """

    def __init__(self,  # pylint: disable=too-many-arguments
                 raw_message_parser: RawMessageParser,
                 sanitizer_handler: SanitizerContainerHandler,
                 sdr_handler: SDRContainerHandler,
                 mdfp_handler: MDFParserContainerHandler,
                 anon_handler: AnonymizeContainerHandler,
                 chc_handler: CHCContainerHandler,
                 sdm_handler: SDMContainerHandler):

        self.__raw_message_parser = raw_message_parser
        self.__container_router: dict[Type[SqsMessage], ContainerHandler] = {
            SanitizerMessage: sanitizer_handler,
            SDRetrieverMessage: sdr_handler,
            MDFParserMessage: mdfp_handler,
            AnonymizeMessage: anon_handler,
            CHCMessage: chc_handler,
            SDMMessage: sdm_handler
        }

    def handle(self, message: MessageTypeDef) -> Request:  # type: ignore
        """
        Runs the respective handler

        Args:
            message (MessageTypeDef): The message arrviing from the queue

        Raises:
            UknownSQSMessage: If the message couldn't be parsed

        Returns:
            Request: The request to be to the handler
        """

        # Parses and retireves the message based on the container
        container_message = self.parse_message(message)
        container_type_message = type(container_message)

        _logger.debug("Message has been parsed into a pydantic model")

        if container_type_message not in self.__container_router:
            raise UnknownSQSMessage(f"A message of type {str(container_type_message)} does not have an handler")

        # Process the message by the correct handler
        container_handler = self.__container_router[container_type_message]
        _logger.debug("Handler %s is going to process the message", str(container_type_message))

        return container_handler.create_request(container_message)

    def parse_message(self, message: MessageTypeDef) -> SqsMessage:
        """
        Converts a message from the queue into an SQSMessage.

        This will apply the following temporary conversions:
        1. Apply a conversion for SNS embedded messages.
        2. Convert messages that are not pydantic models yet.

        Lastly a final parsing will be made assuming that every message can be parsed into a pydantic model

        Args:
            message (MessageTypeDef): _description_

        Returns:
            SqsMessage: _description_
        """

        # ONLY NEEDED FOR SNS EMBEDED MESSAGE
        message_dict = parse_message_body_to_dict(message["Body"])
        if "messageAttributes" in message_dict and "body" in message_dict:
            message["MessageAttributes"] = message_dict["messageAttributes"]
            message["Body"] = message_dict["body"]

            # Capitilizes the message attribute type keys
            for attr_key, attr_dict in message["MessageAttributes"].items():
                new_dict = {}
                for attr_type, attrt_value in attr_dict.items():
                    cap_attr_type = attr_type[0].upper() + attr_type[1:]
                    new_dict[cap_attr_type] = attrt_value

                message["MessageAttributes"][attr_key] = new_dict  # type: ignore

        # Parse raw artifact messages from sdm, mdfparser, anonymize and chc
        parsed_message = self.__raw_message_parser.adapt_message(message)

        _logger.info("Message after temporary parsing %s", parsed_message)

        # Parses into a pydantic model
        return parse_sqs_message(parsed_message)
