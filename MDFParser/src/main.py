import json
import logging
import os
import re
from typing import Any, TypedDict, cast
import boto3
import yaml

from baseaws.shared_functions import ContainerServices, GracefulExit
from .downloader import Downloader
from .uploader import Uploader
from .synchronizer import Synchronizer
from .processor import Processor
from .chc_counter import ChcCounter

CONTAINER_NAME = "MDFParser"    # Name of the current container
CONTAINER_VERSION = "v1.0"      # Version of the current container

_logger: logging.Logger


def main():
    # External configuration that can be configured as kubernetes secret
    config: MdfParserConfig
    with open(os.environ.get('CONFIG_FILE', '/app/config/config.yml'), 'r') as configfile:
        config = cast(MdfParserConfig, yaml.load(configfile, yaml.SafeLoader))

    _logger.info("Starting Container %s (%s)..\n",
                 CONTAINER_NAME, CONTAINER_VERSION)

    # Logic classes
    downloader = Downloader()
    uploader = Uploader()
    synchronizer = Synchronizer()
    chc_counter = ChcCounter()
    processors: list[Processor] = [chc_counter]

    # AWS clients for container_services
    sqs_client = boto3.client('sqs', region_name='eu-central-1')
    container_services = ContainerServices(
        container=CONTAINER_NAME, version=CONTAINER_VERSION)

    # allow graceful exit
    graceful_exit = GracefulExit()

    while(graceful_exit.continue_running):
        _logger.debug('Listening to input queue.')
        message = container_services.listen_to_input_queue(
            sqs_client, config['input_queue'])
        if message:
            # check message has the required fields
            if not('Body' in message and '_id' in message['Body'] and 's3_path' in message['Body']):
                _logger.warning('Required fields are not in the message.')
                continue

            message_body = cast(InputMessage, json.loads(
                message['Body'].replace("\'", "\"")))
            _logger.debug('Processing recording entry %s', message_body['_id'])
            try:
                metadata: dict[str, Any] = {
                    '_id': message_body['_id'],
                }
                metadata.update(process_request(
                    message_body['s3_path'], downloader, uploader, synchronizer, processors))
                container_services.send_message(
                    sqs_client, config['metadata_output_queue'], metadata)
                container_services.delete_message(
                    sqs_client, message['ReceiptHandle'], config['input_queue'])
            except Exception:
                _logger.exception(
                    'Error during processing of request for %s', message_body['_id'])


def process_request(mdf_s3_path: str, downloader: Downloader, uploader: Uploader, synchronizer: Synchronizer, processors: list[Processor]) -> dict[str, Any]:
    _logger.info('Starting processing of metadata for %s', mdf_s3_path)

    # download and synchronize metadata
    mdf = downloader.download(mdf_s3_path)
    timestamp_from, timestamp_to = extract_timestamps(mdf_s3_path)
    synchronized = synchronizer.synchronize(mdf, timestamp_from, timestamp_to)

    # compute updated metadata
    metadata: dict[str, Any] = {}

    successful_processings: int = 0
    for processor in processors:
        try:
            process_output = processor.process(synchronized)
            metadata.update(process_output)
            successful_processings += 1
        except Exception:
            # we do not want the entire recording to fail for a specific processing only
            _logger.exception('Error processing metadata.')
    if(successful_processings == 0):
        raise NoProcessingSuccessfulException(
            'Not a single processing succeeded, therefore not updating metadata.')

    _logger.info('Successfully processed metadata for %s', mdf_s3_path)

    # upload synchronized signals to s3 and store path in metadata
    try:
        metadata['signals_file'] = uploader.upload_signals(
            synchronized, mdf_s3_path)
        # added because Metadata expects this field to exist
        metadata["s3_path"] = metadata['signals_file']['bucket'] + \
            "/"+metadata['signals_file']["key"]
    except Exception:
        _logger.exception('Error uploading synchronized signals to S3.')
        raise

    return metadata


def extract_timestamps(filepath: str) -> tuple[int, int]:
    match = re.search(r"_(\d{13,})_(\d{13,})_", filepath)
    if not match or len(match.groups()) < 2:
        raise InvalidFileNameException(
            'Cannot extract timestamps from filepath "' + filepath + '".')
    timestamp_from = int(match.group(1))
    timestamp_to = int(match.group(2))
    return timestamp_from, timestamp_to


class InvalidFileNameException(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class NoProcessingSuccessfulException(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class MdfParserConfig(TypedDict):
    input_queue: str
    metadata_output_queue: str
    log_level: str


class InputMessage(TypedDict):
    _id: str
    s3_path: str


if __name__ == '__main__':
    # Define configuration for logging messages
    _logger = ContainerServices.configure_logging('mdfparser')
    main()
else:
    _logger = logging.getLogger('mdfparser')
