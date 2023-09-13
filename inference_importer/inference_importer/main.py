""" Main module. """
import os
import boto3
from base import GracefulExit
from base.aws.container_services import ContainerServices
from inference_importer.sqs_message import SQSMessage
from inference_importer.service import InferenceImporter


CONTAINER_NAME = "InferenceImporter"  # Name of the current container
CONTAINER_VERSION = "v1.0"  # Version of the current container
INFERENCE_IMPORTER_QUEUE = os.environ.get("INFERENCE_IMPORTER_QUEUE")
AWS_REGION = os.environ.get("AWS_REGION", "eu-central-1")
TENANT_MAPPING_CONFIG_PATH = os.getenv("TENANT_MAPPING_CONFIG_PATH")


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
    sqs_message = container_services.get_single_message_from_input_queue(sqs_client,
                                                                         input_queue=INFERENCE_IMPORTER_QUEUE)

    if sqs_message:
        _logger.info("Received a message")

        parsed_message = SQSMessage.from_raw_sqs_message(sqs_message)
        processed = importer.process(s3_client, parsed_message)

        # Delete message after processing
        if processed:
            container_services.delete_message(sqs_client, sqs_message["ReceiptHandle"],
                                              input_queue=INFERENCE_IMPORTER_QUEUE)


def main(stop_condition=lambda: True):
    """Main function"""

    # Define configuration for logging messages
    _logger.info("Starting Container %s (%s)..\n",
                 CONTAINER_NAME, CONTAINER_VERSION)

    # Create the necessary clients for AWS services access
    sqs_client = boto3.client("sqs", region_name=AWS_REGION)
    s3_client = boto3.client("s3", region_name=AWS_REGION)

    # Initialise instance of ContainerServices class
    container_services = ContainerServices(container=CONTAINER_NAME,
                                           version=CONTAINER_VERSION)

    importer = InferenceImporter()

    graceful_exit = GracefulExit()

    _logger.info("Listening to input queue...")

    while stop_condition() and graceful_exit.continue_running:
        process_message(container_services, importer, s3_client, sqs_client)


if __name__ == "__main__":
    main()
