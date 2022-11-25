"""SDM container script"""
import json
import boto3
from typing import Optional
from dataclasses import dataclass
from base.aws.container_services import ContainerServices
from pathlib import Path

CONTAINER_NAME = "SDM"          # Name of the current container
CONTAINER_VERSION = "v6.2"      # Version of the current container
VIDEO_FORMATS = ['mp4','avi']
IMAGE_FORMATS = ['jpeg','jpg','png']
MINIMUM_LENGTH = 4              # Minimum length allowed a/b.c

_logger = ContainerServices.configure_logging('sdm')



@dataclass
class FileMetadata():
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
    if len(s3_path)<MINIMUM_LENGTH:
        return FileMetadata(None, None, None)

    pathlib_s3 = Path(s3_path)
    file_format = pathlib_s3.suffix if pathlib_s3.suffix != '' else None
    msp = None
    if s3_path.find('/') > 0:
        msp = str(pathlib_s3.parent)

        file_name = pathlib_s3.name
    else:
        file_name = s3_path

    return FileMetadata(msp, file_name, file_format)


def processing_sdm(container_services, sqs_message):
    """Retrieves the MSP name from the message received and creates
    a relay list for the current file

    Arguments:
        container_services {base.aws.container_services.ContainerServices}
                            -- [class containing the shared aws functions]
        body {string} -- [string containing the body info from
                          the received message]
    Returns:
        relay_data {dict} -- [dict with the relevant info for the file received
                            and to be sent via message to the input queues
                            of the relevant containers]
    """
    relay_data = dict()
    # Converts message body from string to dict
    # (in order to perform index access)
    new_body = sqs_message['Body'].replace("\'", "\"")
    sqs_body = json.loads(new_body)

    # Ignore s3:TestEvent
    if sqs_body.get("Event") == "s3:TestEvent" :
        _logger.info("SQS message is a s3:TestEvent")
        return relay_data

    # Access key value from msg body
    s3_path = sqs_body["Records"][0]["s3"]["object"]["key"]

    # Identify the file in the message
    file_metadata = identify_file(s3_path)
    msp, file_name, file_format = file_metadata.msp, file_metadata.filename, file_format

    # if somehting went wrong with the file parsing
    if msp is None or file_name is None or file_format is None:
        if msp is None:
            _logger.warning("File %s will not be processed - File is outside MSP folders.", file_name)
        if file_name is None:
            _logger.warning("Could not parse file name.")
        if file_format is None:
            _logger.warning("Could not parse file format.")
        _logger.info("Message dump:")
        _logger.info(sqs_message)
        return relay_data

    # TODO: find place to store file formats (video/image) - container_scripts, global var, env var, ...?
    if file_format in VIDEO_FORMATS:

        _logger.info("Processing video message..")
        # Creates relay list to be used by other containers
        relay_data["processing_steps"] = container_services.msp_steps[msp].copy()
        relay_data["s3_path"] = s3_path
        relay_data["data_status"] = "received"
        container_services.display_processed_msg(relay_data["s3_path"])

    elif file_format in IMAGE_FORMATS:
        """ Snapshot processing will only need to go through one stage - anonymization
        because both anony & CHC call upon the same transforming algorithms.
        CHC just generates the json?

        The processing is the same right now, but it might make sense to make it just anonymization for speedup
        """

        _logger.info("Processing snapshot message..")
        # Creates relay list to be used by other containers
        relay_data["processing_steps"] = container_services.msp_steps[msp].copy()
        # to skip image CHC
        relay_data["processing_steps"].remove("CHC")
        relay_data["s3_path"] = s3_path
        relay_data["data_status"] = "received"
        container_services.display_processed_msg(relay_data["s3_path"])

    elif file_format in container_services.raw_s3_ignore:
        _logger.warning("File %s will not be processed - File format '%s' is on the Raw Data S3 ignore list.", file_name, file_format)

    else:
        _logger.warning("File %s will not be processed - File format '%s' is unexpected.", file_name, file_format)

    return relay_data


def log_message(message, queue=CONTAINER_NAME):
    _logger.info("Message contents from %s: [%s]", queue, message)

def main():
    """Main function"""

    # Define configuration for logging messages
    _logger.info("Starting Container %s (%s)..\n", CONTAINER_NAME, CONTAINER_VERSION)

    # Create the necessary clients for AWS services access
    s3_client = boto3.client('s3', region_name='eu-central-1')
    sqs_client = boto3.client('sqs', region_name='eu-central-1')

    # Initialise instance of ContainerServices class
    container_services = ContainerServices(container=CONTAINER_NAME, version=CONTAINER_VERSION)

    # Load global variable values from config json file (S3 bucket)
    container_services.load_config_vars(s3_client)

    _logger.info("\nListening to input queue(s)..\n\n")

    while(True):
        # Check input SQS queue for new messages
        sqs_message = container_services.listen_to_input_queue(sqs_client)

        if sqs_message:
            # save some messages as examples for development
            log_message(sqs_message)

            # Processing step
            relay_list = processing_sdm(container_services, sqs_message)

            # If file received is valid
            if relay_list:

                # Send message to input queue of the next processing step
                # (if applicable)
                if relay_list["processing_steps"]:
                    next_step = relay_list["processing_steps"][0]
                    next_queue = container_services.sqs_queues_list[next_step]
                    container_services.send_message(sqs_client, next_queue, relay_list)

                # Send message to input queue of metadata container
                metadata_queue = container_services.sqs_queues_list["Metadata"]
                container_services.send_message(sqs_client, metadata_queue, relay_list)

            # Delete message after processing
            container_services.delete_message(sqs_client, sqs_message['ReceiptHandle'])


if __name__ == '__main__':
    main()
