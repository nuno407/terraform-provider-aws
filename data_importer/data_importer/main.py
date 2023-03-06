"""DataImporter container script"""
import os

import boto3
from base import GracefulExit
from base.aws.container_services import ContainerServices
from data_importer.fiftyone_importer import FiftyoneImporter
from data_importer.processor_repository import ProcessorRepository
from data_importer.sqs_message import SQSMessage

CONTAINER_NAME = "DataImporter"  # Name of the current container
CONTAINER_VERSION = "v1.0"  # Version of the current container
DATA_IMPORTER_QUEUE = os.environ.get("DATA_IMPORTER_QUEUE")

DATA_OWNER = "IMS"

_logger = ContainerServices.configure_logging(__name__)


def process_message(container_services, importer, s3_client, sqs_client):
    """
    Gets a message from the queue, determines what type of file was uploaded and inserts it as a Fiftyone sample

    :param container_services: container_services instance
    :param importer: Fiftyone importer
    :param s3_client: Boto S3 client to fetch files with
    :param sqs_client: SQS client to get messages
    """
    # Check input SQS queue for new messages
    sqs_message = container_services.get_single_message_from_input_queue(sqs_client, input_queue=DATA_IMPORTER_QUEUE)

    if sqs_message:
        _logger.info("Received a message")

        parsed_message = SQSMessage.from_raw_sqs_message(sqs_message)

        processor = ProcessorRepository.get_processor(parsed_message.file_extension)

        dataset = importer.load_dataset(f"{DATA_OWNER}-{parsed_message.dataset}", [DATA_OWNER])

        metadata = processor.load_metadata(parsed_message, s3_client=s3_client, container_services=container_services)

        # Find or create a new Sample with the given metadata
        processor.upsert_sample(dataset, parsed_message, metadata, importer)

        # Delete message after processing
        container_services.delete_message(sqs_client, sqs_message["ReceiptHandle"], input_queue=DATA_IMPORTER_QUEUE)


def main(stop_condition=lambda: True):
    """Main function"""

    # Define configuration for logging messages
    _logger.info("Starting Container %s (%s)..\n", CONTAINER_NAME, CONTAINER_VERSION)

    # Create the necessary clients for AWS services access
    sqs_client = boto3.client("sqs", region_name="eu-central-1")
    s3_client = boto3.client("s3", region_name="eu-central-1")

    # Initialise instance of ContainerServices class
    container_services = ContainerServices(container=CONTAINER_NAME, version=CONTAINER_VERSION)

    # Load global variable values from config json file (S3 bucket)

    importer = FiftyoneImporter()
    # Loading all processors from processors folder
    ProcessorRepository.load_all_processors()

    graceful_exit = GracefulExit()

    _logger.info("Listening to input queue...")

    while stop_condition() and graceful_exit.continue_running:
        process_message(container_services, importer, s3_client, sqs_client)


if __name__ == "__main__":
    main()