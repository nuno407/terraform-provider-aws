# type: ignore
"""Sensor Data Retriever - V7
- adds ingestion of TrainingRecorder IMU data
"""
import json
import logging as log

from kink import inject

from base.aws.sqs import SQSController
from base.graceful_exit import GracefulExit
from base.model.artifacts import parse_artifact
from sdretriever.bootstrap import bootstrap_di
from sdretriever.constants import CONTAINER_NAME, CONTAINER_VERSION
from sdretriever.handler import IngestionHandler

# Global log message formatting
_logger = log.getLogger("SDRetriever")


def __deserialize(raw_message: str) -> str:
    call_args = [("'", '"'), ("\\n", ""), ("\\\\", ""),  # pylint: disable=invalid-string-quote
                 ("\\", ""), ('"{', "{"), ('}"', "}")]  # pylint: disable=invalid-string-quote
    for args in call_args:
        raw_message = raw_message.replace(args[0], args[1])
    return raw_message


@inject
def main(graceful_exit: GracefulExit,
         sqs_controller: SQSController,
         ingestion_handler: IngestionHandler):
    """Main function of the component.

    Args:
        config (SDRetrieverConfig): configmap of the component
    """

    # Define configuration for logging messages
    _logger.info("Starting Container %s %s", CONTAINER_NAME, CONTAINER_VERSION)

    _logger.info("Waiting for messages...")
    while graceful_exit.continue_running:
        # Poll source (SQS queue) for a new message
        message = sqs_controller.get_message()
        if message:
            _logger.info("Received artifact message -> %s", message)

            raw_message = __deserialize(message["Body"])
            try:
                parsed_body = json.loads(raw_message)
                artifact = parse_artifact(parsed_body["Message"])
                ingestion_handler.handle(artifact, message)
                # the message is deleted in the handler if processed successfully
            # because we never want to crash the container
            except Exception:  # pylint: disable=broad-except
                _logger.exception(
                    "Error parsing artifact, skipping message %s",
                    message["MessageId"])
                continue

    _logger.info("%s exited gracefully.", CONTAINER_NAME)


if __name__ == "__main__":
    # Instanciating main loop and injecting dependencies
    bootstrap_di()
    main()  # pylint: disable=no-value-for-parameter
