"""SDM container script"""
import os
from pathlib import Path

from typing import Optional
from dataclasses import dataclass
import json
from base.aws.container_services import ContainerServices

from base.constants import IMAGE_FORMATS, VIDEO_FORMATS
import boto3


CONTAINER_NAME = "SDM"          # Name of the current container
CONTAINER_VERSION = "v6.2"      # Version of the current container
MINIMUM_LENGTH = 4              # Minimum length allowed a/b.c

_logger = ContainerServices.configure_logging("sdm")


@dataclass
class FileMetadata():
    """ class for file metadata
    Contains the file directory(path), file name and file format
    """
    msp: Optional[str]
    filename: Optional[str]
    file_format: Optional[str]


def identify_file(s3_path: str) -> FileMetadata:
    """Identifies properties for S3 paths.

    Args:
        s3_path (str): S3 full path to be parsed

    Returns:
        FileMetadata: object containing file metadata
    """
    if len(s3_path) < MINIMUM_LENGTH:
        return FileMetadata(None, None, None)

    pathlib_s3 = Path(s3_path)
    file_format = pathlib_s3.suffix.strip(".") if pathlib_s3.suffix != "" else None
    msp = None
    if s3_path.find("/") > 0:
        msp = str(pathlib_s3.parent)

        file_name = pathlib_s3.name
    else:
        file_name = s3_path

    return FileMetadata(msp, file_name, file_format)


def get_processing_steps_for_msp(container_services: ContainerServices, msp: str) -> list[str]:
    """Get processing steps for MSP

    Args:
        container_services (ContainerServices): container services reference
        msp (str): managed service provider key

    Returns:
        list[str]: list of processing steps
    """
    return container_services.msp_steps[msp].copy(
    ) if msp in container_services.msp_steps else container_services.msp_steps["default"].copy()


def is_identify_successful(metadata: FileMetadata) -> bool:
    """Check if file identification was succesful

    Args:
        metadata (FileMetadata): ingested file metadata

    Returns:
        bool: true if identification was succesful
    """
    if metadata.msp is None:
        _logger.warning("%s not be processed - File is outside MSP folders.", metadata.filename)
    if metadata.filename is None:
        _logger.warning("Could not parse file name.")
    if metadata.file_format is None:
        _logger.warning("Could not parse file format.")

    return bool(metadata.msp is not None and metadata.filename
                is not None and metadata.file_format is not None)


def processing_sdm(container_services, sqs_client, sqs_message):
    """Retrieves the MSP name from the message received and creates
    a relay list for the current file

    Arguments:
        container_services {base.aws.container_services.ContainerServices}
                            -- [class containing the shared aws functions]
        body {dict} -- [dict with the sqs message triggered by the s3 event
                            in the anon bucket]
    Returns:
        relay_data {dict} -- [dict with the relevant info for the file received
                            and to be sent via message to the input queues
                            of the relevant containers]
    """
    relay_data = {}
    # Converts message body from string to dict
    # (in order to perform index access)
    new_body = sqs_message["Body"].replace("\"", "\"")
    sqs_body = json.loads(new_body)

    # Ignore s3:TestEvent
    if sqs_body.get("Event") == "s3:TestEvent":
        _logger.info("SQS message is a s3:TestEvent")
        return relay_data

    # Access key value from msg body
    s3_path = sqs_body["Records"][0]["s3"]["object"]["key"]

    # Identify the file in the message
    metadata = identify_file(s3_path)

    # if somehting went wrong with the file parsing
    if not is_identify_successful(metadata):
        _logger.info("Message dump:")
        _logger.info(sqs_message)
        return relay_data

    if metadata.file_format in VIDEO_FORMATS:

        _logger.info("Processing video message..")
        # Creates relay list to be used by other containers
        relay_data["processing_steps"] = get_processing_steps_for_msp(container_services, metadata.msp)
        relay_data["s3_path"] = s3_path
        relay_data["data_status"] = "received"
        container_services.display_processed_msg(relay_data["s3_path"])

    elif metadata.file_format in IMAGE_FORMATS:
        # Snapshot processing will only need to go through one stage - anonymization
        # because both anony & CHC call upon the same transforming algorithms.
        # CHC just generates the json?
        # The processing is the same right now, but it might make sense to make it just anonymization for speedup

        _logger.info("Processing snapshot message..")
        # Creates relay list to be used by other containers
        relay_data["processing_steps"] = get_processing_steps_for_msp(container_services, metadata.msp)
        # to skip image CHC
        relay_data["processing_steps"].remove("CHC")
        relay_data["s3_path"] = s3_path
        relay_data["data_status"] = "received"
        container_services.display_processed_msg(relay_data["s3_path"])

    elif metadata.file_format in container_services.raw_s3_ignore:
        _logger.warning(
            "File %s will not be processed - File format '%s' is on the Raw Data S3 ignore list.",
            metadata.filename,
            metadata.file_format)

    else:
        raise ValueError(f"File '{metadata.filename}' will not be processed - \
                           File format '{metadata.file_format}' is unexpected.")

    # If file received is valid
    if relay_data:

        # Send message to input queue of the next processing step
        # (if applicable)
        if relay_data["processing_steps"]:
            next_step = relay_data["processing_steps"][0]
            next_queue = container_services.sqs_queues_list[next_step]
            container_services.send_message(sqs_client, next_queue, relay_data)

        # Send message to input queue of metadata container
        metadata_queue = container_services.sqs_queues_list["Metadata"]
        container_services.send_message(sqs_client, metadata_queue, relay_data)

    return relay_data


def main(stop_condition=lambda: True):
    """Main function"""

    # Define configuration for logging messages
    _logger.info("Starting Container %s (%s)..\n", CONTAINER_NAME, CONTAINER_VERSION)

    # Create the necessary clients for AWS services access
    sqs_client = boto3.client("sqs", region_name="eu-central-1", endpoint_url=os.getenv("AWS_ENDPOINT", None))

    # Initialise instance of ContainerServices class
    container_services = ContainerServices(container=CONTAINER_NAME, version=CONTAINER_VERSION)

    # Load global variable values from config json file (S3 bucket)
    container_services.load_config_vars()

    _logger.info("\nListening to input queue(s)..\n\n")

    while stop_condition():
        # Check input SQS queue for new messages
        sqs_message = container_services.get_single_message_from_input_queue(sqs_client)

        if sqs_message:
            # save some messages as examples for development
            _logger.info("Message contents from %s: [%s]", CONTAINER_NAME, sqs_message)

            # Processing step
            processing_sdm(container_services, sqs_client, sqs_message)

            # Delete message after processing
            container_services.delete_message(sqs_client, sqs_message["ReceiptHandle"])


if __name__ == "__main__":
    main()
