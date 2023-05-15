"""Consumer Class"""
import json
import logging
from typing import Any, Callable, Optional

from kink import inject
from mypy_boto3_sqs import SQSClient
from base.aws.container_services import ContainerServices
from base.model.artifacts import parse_artifact
from mdfparser.config import MdfParserConfig
from mdfparser.constants import DataType
from mdfparser.exceptions import HandlerTypeNotExist
from mdfparser.interfaces.artifact_adapter import ArtifactAdapter
from mdfparser.interfaces.handler import Handler
from mdfparser.interfaces.input_message import InputMessage
from mdfparser.interfaces.output_message import OutputMessage


_logger = logging.getLogger("mdfparser." + __name__)


@inject
class Consumer:
    """
    Consumer Class
    Responsible for Handling and routing the SQS messagges to the specific handlers.

    """

    def __init__(  # pylint: disable=too-many-arguments
            self,
            container_services: ContainerServices,
            handler_list: list[Handler],
            config: MdfParserConfig,
            adapter: ArtifactAdapter,
            sqs_client: SQSClient):
        """
        Creates the consumer class that acts as a router to multiple handlers.

        Args:
            container_services (ContainerServices): Container services object
            handler_list (list[Handler]): A list of handler where the messages should be routed.
            config: (MdfParserConfig): The configmap of the MDFParser.
            adapter: (ArtifactAdapter): The artifact adapter.
            sqs_client: (SQSClient): The SQS client to be used.
        """
        self.message_adapter = adapter
        self.config = config
        self.sqs_client = sqs_client
        self.container_services = container_services
        self.handlers: dict[DataType, Handler] = {
            handler.handler_type(): handler for handler in handler_list}

    def consume_msg(self, msg: dict[str, Any]) -> Optional[OutputMessage]:
        """
        Consumes a message body arriving from SDR.

        Args:
            msg (dict): This is the Body of the SQS message already unserilized.

        Returns:
            Optional[OutputMessage]: Returns a message if it needs to be sent to the Metadata.
        """
        parsed_artifact = parse_artifact(msg)
        input_message = self.message_adapter.adapt_message(parsed_artifact)
        handler = self.__get_handler(input_message)
        _logger.info("Message type identified as %s",
                     input_message.data_type.value)

        return handler.ingest(input_message)

    def __get_handler(self, msg: InputMessage) -> Handler:
        """
        Grabs the respective handler for the message.

        Args:
            msg (InputMessage): The input message recieved

        Raises:
            HandlerTypeNotExist: If the respective handler does not exist

        Returns:
            Handler: An handler to process the message
        """
        handler: Optional[Handler] = self.handlers.get(msg.data_type, None)

        if handler is None:
            raise HandlerTypeNotExist(
                f"Handler {msg.data_type} does not exist. "
                f"The message received needs to have a known data type ({str(self.handlers.keys())})")

        return handler

    def __send_message_to_metadata(self, msg: OutputMessage) -> None:
        """
        Sends a message to the metadata queue.

        Args:
            msg (OutputMessage): Message to be sent.
        """
        json_msg = msg.to_json()
        _logger.info("Sending message %s to metadata queue", json_msg)
        self.container_services.send_message(
            self.sqs_client, self.config.metadata_output_queue, json_msg)

    # type: ignore
    def run(self, graceful_exit: Callable[..., bool] = lambda x: x) -> None:
        """
        Main loop done in the queues.

        Args:
            graceful_exit (Callable, optional): Used for tests. Defaults to True..
        """
        while graceful_exit(True):
            message = self.container_services.get_single_message_from_input_queue(
                self.sqs_client, self.config.input_queue)

            if not message:
                continue

            _logger.info("Processing message with body %s", str(message))
            message_body = json.loads(
                message.get("Body", "").replace("\'", "\""))
            output_msg = self.consume_msg(message_body)

            if output_msg:
                self.__send_message_to_metadata(output_msg)
            else:
                _logger.warning(
                    "Output message is empty and will not be sent to metadata")
            self.container_services.delete_message(
                self.sqs_client,
                message["ReceiptHandle"],
                self.config.input_queue)
