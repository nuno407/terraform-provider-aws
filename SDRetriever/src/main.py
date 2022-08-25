"""Sensor Data Retriever - V6
- removed metadata processing
- improved logging
- improved code readibility
"""

import logging as log

import boto3
from ingestor import MetadataIngestor, SnapshotIngestor, VideoIngestor
from message import SnapshotMessage, VideoMessage
from sourcecommuter import SourceCommuter

from baseaws.shared_functions import ContainerServices, GracefulExit, StsHelper

CONTAINER_NAME = "SDRetriever" # Name of the current container
CONTAINER_VERSION = "v6" # Version of the current container
METADATA_FILE_EXT = '_metadata_full.json' # file format for metadata stored on DevCloud raw S3
VIDEO = ["InteriorRecorder", "TrainingRecorder", "FrontRecorder"] # Known types of video recorder services (SRX)
IMAGE = ["TrainingMultiSnapshot"] # Known types of snapshot recorder services (SRX)

log.basicConfig(format='%(levelname)s %(message)s') # Global log message formatting
LOGGER = log.getLogger("SDRetriever")
LOGGER.setLevel(log.INFO)
formatter = log.Formatter('%(messageid)-15s %(levelname)s :: %(message)s')
handler = log.StreamHandler()
handler.setFormatter(formatter)
handler.setLevel(log.INFO)
LOGGER.addHandler(handler)
LOGGER.propagate = False

def message_type_identifier(message: dict):
    """ Identify if the type of the media described in the message.
        Only returns a type when its sure its from that type otherwise returns None.

    Args:
        message (dict): message to identify

    Returns:
        result (str): type identified, defaults to None.
    """
    message = str(message)
    result = None

    # Checks if the message contains *just* InteriorRecorder.
    if message.find("InteriorRecorder") != -1 and message.find("TrainingRecorder") == -1 and message.find("TrainingMultiSnapshot") == -1 and message.find("FrontRecorder") == -1:
        result = "InteriorRecorder"

    # Checks if the message contains *just* TrainingRecorder.
    elif message.find("TrainingRecorder") != -1 and message.find("InteriorRecorder") == -1 and message.find("TrainingMultiSnapshot") == -1 and message.find("FrontRecorder") == -1:
        result = "TrainingRecorder"

    # Checks if the message contains *just* TrainingMultiSnapshot.
    elif message.find("TrainingMultiSnapshot") != -1 and message.find("TrainingRecorder") == -1 and message.find("InteriorRecorder") == -1 and message.find("FrontRecorder") == -1:
        result = "TrainingMultiSnapshot"

    # Checks if the message contains *just* FrontRecorder.
    elif message.find("FrontRecorder") != -1 and message.find("TrainingRecorder") == -1 and message.find("TrainingMultiSnapshot") == -1 and message.find("InteriorRecorder") == -1:
        result = "FrontRecorder"

    return result

def main():

    # Define configuration for logging messages
    log.info(f"Starting Container {CONTAINER_NAME} {CONTAINER_VERSION}")

    # Start necessary services
    S3_CLIENT = boto3.client('s3', region_name='eu-central-1')
    SQS_CLIENT = boto3.client('sqs', region_name='eu-central-1')
    STS_CLIENT = boto3.client('sts', region_name='eu-central-1')
    CS = ContainerServices(container=CONTAINER_NAME, version=CONTAINER_VERSION)
    CS.load_config_vars(S3_CLIENT)
    STS_HELPER = StsHelper(STS_CLIENT, role=CS.rcc_info.get("role"), role_session="DevCloud-SDRetriever")
    GRACEFUL_EXIT = GracefulExit()

    # Create file ingestors
    METADATA_ING = MetadataIngestor(container_services=CS, s3_client=S3_CLIENT, sqs_client=SQS_CLIENT, sts_helper=STS_HELPER)
    SNAPSHOT_ING = SnapshotIngestor(container_services=CS, s3_client=S3_CLIENT, sqs_client=SQS_CLIENT, sts_helper=STS_HELPER)
    VIDEO_ING = VideoIngestor(container_services=CS, s3_client=S3_CLIENT, sqs_client=SQS_CLIENT, sts_helper=STS_HELPER)

    # Create source commuter
    SRC = SourceCommuter([CS.sqs_queues_list['SDRetriever'], CS.sqs_queues_list['Selector']])

    metadata_queue = CS.sqs_queues_list["Metadata"]
    
    while(GRACEFUL_EXIT.continue_running):

        # Poll source (SQS queue) for a new message
        source = SRC.get_source()
        message = CS.listen_to_input_queue(SQS_CLIENT, source)

        if message:

            # Identify the message
            identity = message_type_identifier(message)
            LOGGER.info(f"Pulled a message from {source} ({identity}) -> {message}", extra={"messageid": message.get('MessageId')})

            if identity in VIDEO:

                # Parse raw message
                msg_obj = VideoMessage(message)

                # If ingestable 
                if msg_obj.validate():

                    # Process parsed message
                    db_record_data, request_metadata = VIDEO_ING.ingest(msg_obj)
                    
                    # If metadata is to be downloaded - it's an interior recorder video
                    if request_metadata:
                        source_data = METADATA_ING.ingest(msg_obj)

                        # If we successfully ingested metadata, update the record
                        if source_data:
                            db_record_data.update({"MDF_available": "Yes", "sync_file_ext": METADATA_FILE_EXT})
                    # Send message to input queue of metadata container with the record data
                    CS.send_message(SQS_CLIENT, metadata_queue, db_record_data)
                    LOGGER.info(f"Message sent to {metadata_queue}", extra={"messageid": message.get('MessageId')})    
                CS.delete_message(SQS_CLIENT, msg_obj.receipthandle, source)
                LOGGER.info(f"Message deleted from {source}", extra={"messageid": message.get('MessageId')})


            elif identity in IMAGE:

                # Parse raw message
                msg_obj = SnapshotMessage(message)

                # If ingestable 
                if msg_obj.validate():
                
                    # Process parsed message
                    reprocess = SNAPSHOT_ING.ingest(msg_obj)
                    if not reprocess:
                        CS.delete_message(SQS_CLIENT, msg_obj.receipthandle, source)
                        LOGGER.info(f"Message deleted from {source}", extra={"messageid": message.get('MessageId')})
                else:
                    CS.delete_message(SQS_CLIENT, msg_obj.receipthandle, source)
                    LOGGER.info(f"Message deleted from {source}", extra={"messageid": message.get('MessageId')})
            else:
                LOGGER.error(f"Could not identify message type as video nor snapshot related, skipping", extra={"messageid": message.get('MessageId')})

        # if no message was obtained from the current source
        else:
            SRC.next()
    log.info(f'{CONTAINER_NAME} exited gracefully.')

if __name__ == "__main__":
    main()
