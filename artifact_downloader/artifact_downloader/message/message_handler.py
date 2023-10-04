""" Message handler module """
from kink import inject
from mypy_boto3_sqs.type_defs import MessageTypeDef
from artifact_downloader.message.raw_message_parser import RawMessageParser
from artifact_downloader.container_handlers.handler import ContainerHandler
from artifact_downloader.exceptions import UknownSQSMessage
from artifact_downloader.container_handlers import SanitizerContainerHandler ,AnonymizeContainerHandler, CHCContainerHandler ,SDMContainerHandler ,SDRContainerHandler ,MDFParserContainerHandler
from artifact_downloader.message.incoming_messages import parse_sqs_message, SanitizerMessage, SDRetrieverMessage, MDFParserMessage, AnonymizeMessage, CHCMessage, SDMMessage, SqsMessage
from requests import Request

@inject
class MessageHandler:  # pylint: disable=too-few-public-methods
    """ Coordinates the parsing, transformation and enrichment of messages to REST requests """

    def __init__(self,
                 raw_message_parser: RawMessageParser,
                 sanitizer_handler: SanitizerContainerHandler,
                 sdr_handler: SDRContainerHandler,
                 mdfp_handler: MDFParserContainerHandler,
                 anon_handler: AnonymizeContainerHandler,
                 chc_handler: CHCContainerHandler,
                 sdm_handler: SDMContainerHandler):

        self.__raw_message_parser = raw_message_parser
        self.__container_router : dict[SqsMessage, ContainerHandler] = {
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


        # Parse raw artifact messages from sdm, mdfparser, anonymize and chc
        parsed_message = self.__raw_message_parser.parse_message(message)

        # Parses and retireves the message based on the container
        container_message = parse_sqs_message(parsed_message)
        container_type_message = type(container_message)

        if container_type_message not in self.__container_router:
            raise UknownSQSMessage(f"A message of type {container_type_message} does not have an handler")

        # Process the message by the correct handler
        return self.__container_router[container_type_message].create_request(container_type_message)


