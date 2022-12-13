"""Sensor Data Retriever - V6
- removed metadata processing
- improved logging
- improved code readibility
"""

import logging as log
import os

import boto3

from base import GracefulExit
from base.aws.container_services import ContainerServices
from base.aws.shared_functions import StsHelper
from sdretriever.config import SDRetrieverConfig
from sdretriever.ingestor import MetadataIngestor
from sdretriever.ingestor import SnapshotIngestor
from sdretriever.ingestor import VideoIngestor
from sdretriever.message import SnapshotMessage
from sdretriever.message import VideoMessage
from sdretriever.sourcecommuter import SourceCommuter

CONTAINER_NAME = "SDRetriever"  # Name of the current container
CONTAINER_VERSION = "v6"  # Version of the current container
# file format for metadata stored on DevCloud raw S3
METADATA_FILE_EXT = '_metadata_full.json'
# Known types of video recorder services (SRX)
VIDEO = ["InteriorRecorder", "TrainingRecorder", "FrontRecorder"]
# Video recorder services that include metadata
VIDEO_WITH_METADATA = ["InteriorRecorder"]
# Known types of snapshot recorder services (SRX)
IMAGE = ["TrainingMultiSnapshot"]
MESSAGE_VISIBILITY_EXTENSION_HOURS = [0.5, 3, 12, 12]


# Global log message formatting
LOGGER = log.getLogger("SDRetriever")
ContainerServices.configure_logging('SDRetriever')


def message_type_identifier(message: dict):
    """ Identify if the type of the media described in the message.
        Only returns a type when its sure its from that type otherwise returns None.

    Args:
        message (dict): message to identify

    Returns:
        result (str): type identified, defaults to None.
    """
    _message = str(message)
    result = None

    # Checks if the message contains *just* InteriorRecorder.
    if _message.find("InteriorRecorder") != -1 and _message.find("TrainingRecorder") == - \
            1 and _message.find("TrainingMultiSnapshot") == -1 and _message.find("FrontRecorder") == -1:
        result = "InteriorRecorder"

    # Checks if the message contains *just* TrainingRecorder.
    elif _message.find("TrainingRecorder") != -1 and _message.find("InteriorRecorder") == -1 and _message.find("TrainingMultiSnapshot") == -1 and _message.find("FrontRecorder") == -1:
        result = "TrainingRecorder"

    # Checks if the message contains *just* TrainingMultiSnapshot.
    elif _message.find("TrainingMultiSnapshot") != -1 and _message.find("TrainingRecorder") == -1 and _message.find("InteriorRecorder") == -1 and _message.find("FrontRecorder") == -1:
        result = "TrainingMultiSnapshot"

    # Checks if the message contains *just* FrontRecorder.
    elif _message.find("FrontRecorder") != -1 and _message.find("TrainingRecorder") == -1 and _message.find("TrainingMultiSnapshot") == -1 and _message.find("InteriorRecorder") == -1:
        result = "FrontRecorder"

    return result


def main(config: SDRetrieverConfig):

    # Define configuration for logging messages
    log.info(f"Starting Container {CONTAINER_NAME} {CONTAINER_VERSION}")

    # Start necessary services
    S3_CLIENT = boto3.client('s3', region_name='eu-central-1')
    SQS_CLIENT = boto3.client('sqs', region_name='eu-central-1')
    STS_CLIENT = boto3.client('sts', region_name='eu-central-1')
    CS = ContainerServices(container=CONTAINER_NAME, version=CONTAINER_VERSION)
    CS.load_config_vars(S3_CLIENT)
    STS_HELPER = StsHelper(STS_CLIENT, role=CS.rcc_info.get(
        "role"), role_session="DevCloud-SDRetriever")
    GRACEFUL_EXIT = GracefulExit()

    # Create file ingestors
    METADATA_ING = MetadataIngestor(container_services=CS, s3_client=S3_CLIENT,
                                    sqs_client=SQS_CLIENT, sts_helper=STS_HELPER)
    SNAPSHOT_ING = SnapshotIngestor(container_services=CS, s3_client=S3_CLIENT,
                                    sqs_client=SQS_CLIENT, sts_helper=STS_HELPER)
    VIDEO_ING = VideoIngestor(container_services=CS, s3_client=S3_CLIENT, sqs_client=SQS_CLIENT,
                              sts_helper=STS_HELPER, frame_buffer=config.frame_buffer)

    # Create source commuter
    SRC = SourceCommuter(
        [CS.sqs_queues_list['SDRetriever'], CS.sqs_queues_list['Selector']])

    metadata_queue = CS.sqs_queues_list["Metadata"]

    while (GRACEFUL_EXIT.continue_running):

        # Poll source (SQS queue) for a new message
        source = SRC.get_source()
        message = CS.listen_to_input_queue(SQS_CLIENT, source)

        if message:

            # Identify the message
            identity = message_type_identifier(message)
            LOGGER.info(f"Pulled a message from {source} ({identity}) -> {message}", extra={
                        "messageid": message.get('MessageId')})

            if identity in VIDEO:

                # Parse raw message
                video_msg_obj = VideoMessage(message)

                # If the message is irrelevant to us, we can delete it imediately
                if video_msg_obj.is_irrelevant(config.tenant_blacklist, config.recorder_blacklist):
                    LOGGER.info(f"Message deemed irrelevant to us", extra={
                        "messageid": message.get('MessageId')})

                    CS.delete_message(
                        SQS_CLIENT, video_msg_obj.receipthandle, source)
                    continue

                # If ingestable
                if video_msg_obj.validate():
                    request_metadata = identity in VIDEO_WITH_METADATA

                    if request_metadata:
                        # check if metadata is fully available before ingesting video
                        metadata_is_complete, metadata_chunks = METADATA_ING.check_metadata_exists_and_is_complete(
                            video_msg_obj)
                        if not metadata_is_complete:
                            # in case it is not available yet, prolong the message visibility timeout
                            # and put it back in the queue
                            receive_count = video_msg_obj.receive_count
                            prolong_time = MESSAGE_VISIBILITY_EXTENSION_HOURS[min(
                                receive_count, len(MESSAGE_VISIBILITY_EXTENSION_HOURS) - 1)] * 3600
                            LOGGER.warning(
                                f"Metadata not available yet, prolonging message visibility timeout for {prolong_time} seconds", extra={
                                    "messageid": message.get('MessageId')})
                            CS.update_message_visibility(
                                SQS_CLIENT, video_msg_obj.receipthandle, prolong_time, source)
                            continue

                    # Process parsed message
                    db_record_data = VIDEO_ING.ingest(
                        video_msg_obj, config.training_whitelist, config.request_training_upload)
                    # If metadata is to be downloaded - it's an interior recorder video
                    if db_record_data and request_metadata:
                        source_data = METADATA_ING.ingest(
                            video_msg_obj, db_record_data['_id'], metadata_chunks)

                        # If we successfully ingested metadata, update the record
                        if source_data:
                            # we have metadata
                            db_record_data.update(
                                {"MDF_available": "Yes", "sync_file_ext": METADATA_FILE_EXT})
                            # Send message to input queue of metadata container with the record data
                            CS.send_message(
                                SQS_CLIENT, metadata_queue, db_record_data)
                            LOGGER.info(f"Message sent to {metadata_queue}", extra={
                                        "messageid": message.get('MessageId')})
                            # delete from input queue
                            CS.delete_message(
                                SQS_CLIENT, video_msg_obj.receipthandle, source)
                            LOGGER.info(f"Message deleted from {source}", extra={
                                        "messageid": message.get('MessageId')})

                    elif db_record_data and not request_metadata:  # Training recorders
                        # Send message to input queue of metadata container with the record data
                        CS.send_message(
                            SQS_CLIENT, metadata_queue, db_record_data)
                        LOGGER.info(f"Message sent to {metadata_queue}", extra={
                                    "messageid": message.get('MessageId')})
                        # delete from input queue
                        CS.delete_message(
                            SQS_CLIENT, video_msg_obj.receipthandle, source)
                        LOGGER.info(f"Message deleted from {source}", extra={
                                    "messageid": message.get('MessageId')})
                else:
                    # non-parseable messages should go to DLQ
                    LOGGER.debug(f"Message deemed invalid for ingestion, ignoring", extra={
                                 "messageid": message.get('MessageId')})

            elif identity in IMAGE:

                # Parse raw message
                snap_msg_obj = SnapshotMessage(message)

                # If the message is irrelevant to us, we can delete it imediately
                if snap_msg_obj.is_irrelevant(config.tenant_blacklist):
                    LOGGER.info(f"Message deemed irrelevant to us", extra={
                        "messageid": message.get('MessageId')})

                    CS.delete_message(
                        SQS_CLIENT, snap_msg_obj.receipthandle, source)
                    continue

                # If ingestable
                if snap_msg_obj.validate():

                    # Process parsed message
                    reprocess = SNAPSHOT_ING.ingest(snap_msg_obj)
                    if reprocess:
                        LOGGER.info(f"Message will be re-ingested later",
                                    extra={"messageid": snap_msg_obj.messageid})
                    else:
                        CS.delete_message(
                            SQS_CLIENT, snap_msg_obj.receipthandle, source)
                        LOGGER.info(f"Message deleted from {source}", extra={
                                    "messageid": message.get('MessageId')})
                else:
                    # non-parseable messages should go to DLQ
                    LOGGER.debug(f"Message deemed invalid for ingestion, ignoring", extra={
                                 "messageid": message.get('MessageId')})
            else:
                LOGGER.error(f"Could not identify message type as video nor snapshot related, ignoring", extra={
                             "messageid": message.get('MessageId')})

        # if no message was obtained from the current source
        else:
            SRC.next()
    log.info(f'{CONTAINER_NAME} exited gracefully.')


if __name__ == "__main__":
    _config = SDRetrieverConfig.load_config_from_yaml_file(
        os.environ.get('CONFIG_FILE', '/app/config/config.yml'))

    # Instanciating main loop and injecting dependencies
    main(config=_config)
