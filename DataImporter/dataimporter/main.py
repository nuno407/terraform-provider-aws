"""SDM container script"""
import json
import boto3
import os
from base.aws.container_services import ContainerServices
from dataclasses import dataclass, field
from typing import Optional
from typing import List
from pathlib import Path

CONTAINER_NAME = "DataImporter" # Name of the current container
CONTAINER_VERSION = "v1.0"      # Version of the current container
DATA_IMPORTER_QUEUE = os.environ.get("DATA_IMPORTER_QUEUE")

_logger = ContainerServices.configure_logging('dataimporter')


# Class for parsed SQS Message
@dataclass
class SQSMessage():
    principal_id: str
    bucket_name: str
    file_path: str
    file_extension: str

    # Logs the message
    def print_message(self):
        _logger.info("Message")
        _logger.info("principal_id: %s", self.principal_id)
        _logger.info("bucket_name: %s", self.bucket_name)
        _logger.info("file_path: %s", self.file_path)
        _logger.info("file_extension: %s", self.file_extension)


# Class for parsed SQS Message Batch
@dataclass
class SQSMessageBatch():
    messages: List[SQSMessage] = field(default_factory=list) # list of SQS Messages
    batch_size: int = 0 # batch_size defaults to 0

    # Add parsed SQS Message to batch
    def add_message(self, sqsmessage : SQSMessage):
        self.messages.append(sqsmessage)
        self.batch_size = self.batch_size + 1

    # Gets batch size
    def get_batch_size(self) -> int:
        return self.batch_size

    # Logs the messages content
    def print_messages(self):
        for message in self.messages:
            message.print_message()


# Parses a SQS Message
def message_parsing(sqs_message) -> SQSMessage:
    body = sqs_message['Body'].replace("\'", "\"")
    sqs_body = json.loads(body)

    principal_id = sqs_body["Records"][0]["userIdentity"]["principalId"]

    bucket_name = sqs_body["Records"][0]["s3"]["bucket"]["name"]
    file_path = Path(sqs_body["Records"][0]["s3"]["object"]["key"])
    file_extension = file_path.suffix.strip(".") if file_path.suffix != '' else None

    return SQSMessage(principal_id, bucket_name, file_path, file_extension)


def main(stop_condition=lambda: True):
    """Main function"""

    # Define configuration for logging messages
    _logger.info("Starting Container %s (%s)..\n", CONTAINER_NAME, CONTAINER_VERSION)

    # Create the necessary clients for AWS services access
    sqs_client = boto3.client('sqs', region_name='eu-central-1')

    # Initialise instance of ContainerServices class
    container_services = ContainerServices(container=CONTAINER_NAME, version=CONTAINER_VERSION)

    # Load global variable values from config json file (S3 bucket)

    _logger.info("\nListening to input queue(s)..\n\n")

    while stop_condition():
        # Check input SQS queue for new messages
        sqs_messages = container_services.get_multiple_messages_from_input_queue(sqs_client, input_queue=DATA_IMPORTER_QUEUE, max_number_of_messages = 10)

        _logger.info("Batch received!")

        message_batch = SQSMessageBatch()

        for message in sqs_messages:
            # save some messages as examples for development
            _logger.info("Message contents from %s: [%s]", CONTAINER_NAME, message)

            # Processing step
            parsed_message = message_parsing(message)
            message_batch.add_message(parsed_message)

            # Delete message after processing
            container_services.delete_message(sqs_client, message['ReceiptHandle'], input_queue=DATA_IMPORTER_QUEUE)

        message_batch.print_messages()

if __name__ == '__main__':
    main()
