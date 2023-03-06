"""Sensor Data Retriever - V7
- adds ingestion of TrainingRecorder IMU data
"""

import logging as log
import os
from typing import Optional

import boto3
from base import GracefulExit
from base.aws.container_services import ContainerServices
from base.aws.shared_functions import StsHelper
from sdretriever.config import SDRetrieverConfig
from sdretriever.ingestor import (IMUIngestor, MetadataIngestor,
                                  SnapshotIngestor, VideoIngestor)
from sdretriever.message import Message, SnapshotMessage, VideoMessage
from sdretriever.sourcecommuter import SourceCommuter

CONTAINER_NAME = "SDRetriever"  # Name of the current container
CONTAINER_VERSION = "v7"  # Version of the current container
TRAINING_RECORDER = "TrainingRecorder"
FRONT_RECORDER = "FrontRecorder"
INTERIOR_RECORDER = "InteriorRecorder"
SNAPSHOT = "TrainingMultiSnapshot"

# file format for metadata stored on DevCloud raw S3
METADATA_FILE_EXT = '_metadata_full.json'
MESSAGE_VISIBILITY_EXTENSION_HOURS = [0.5, 3, 12, 12]

# Global log message formatting
LOGGER = log.getLogger("SDRetriever")
ContainerServices.configure_logging('SDRetriever')


class IngestorHandler:
    """Handler logic for SDR ingestion scenarios
    """

    def __init__(
            self,
            imu_ing: IMUIngestor,
            metadata_ing: MetadataIngestor,
            video_ing: VideoIngestor,
            snap_ing: SnapshotIngestor,
            cont_services: ContainerServices,
            config: SDRetrieverConfig,
            sqs_client):
        """IngestorHandler init

        Args:
            imu_ing (IMUIngestor): instance of IMUIngestor
            metadata_ing (MetadataIngestor): instance of MetadataIngestor
            video_ing (VideoIngestor): instance of VideoIngestor
            snap_ing (SnapshotIngestor): instance of SnapshotIngestor
            cont_services (ContainerServices): instance of ContainerServices
            config (SDRetrieverConfig): instance of SDRetrieverConfig
            sqs_client (boto3 SQS): instance of boto3 SQS client
        """
        self.metadata_ing = metadata_ing
        self.video_ing = video_ing
        self.cont_services = cont_services
        self.sqs_client = sqs_client
        self.config = config
        self.imu_ing = imu_ing
        self.snap_ing = snap_ing
        self.metadata_queue = cont_services.sqs_queues_list["Metadata"]
        self.hq_request_queue = cont_services.sqs_queues_list["HQ_Selector"]

    @staticmethod
    def message_type_identifier(message: dict) -> Optional[str]:
        """ Identify if the type of the media described in the message.
        Only returns a type when its sure its from that type otherwise returns None.

        Args:
            message (dict): message to identify

        Returns:
            result (Optional[str]): type identified, defaults to None.
        """
        _message = str(message)

        # Checks if the message contains *just* InteriorRecorder.
        if all([
                _message.find("InteriorRecorder") != -1,
                _message.find("TrainingRecorder") == -1,
                _message.find("TrainingMultiSnapshot") == -1,
                _message.find("FrontRecorder") == -1]):
            return INTERIOR_RECORDER

        # Checks if the message contains *just* TrainingRecorder.
        if all([
                _message.find("TrainingRecorder") != -1,
                _message.find("InteriorRecorder") == -1,
                _message.find("TrainingMultiSnapshot") == -1,
                _message.find("FrontRecorder") == -1]):
            return TRAINING_RECORDER

        # Checks if the message contains *just* TrainingMultiSnapshot.
        if all([
                _message.find("TrainingMultiSnapshot") != -1,
                _message.find("TrainingRecorder") == -1,
                _message.find("InteriorRecorder") == -1,
                _message.find("FrontRecorder") == -1]):
            return SNAPSHOT

        # Checks if the message contains *just* FrontRecorder.
        if all([
                _message.find("FrontRecorder") != -1,
                _message.find("TrainingRecorder") == -1,
                _message.find("TrainingMultiSnapshot") == -1,
                _message.find("InteriorRecorder") == -1]):
            return FRONT_RECORDER
        return None

    def message_ingestable(self, message: VideoMessage, source: str) -> bool:
        """ Runs validation and relevancy checks for a VideoMessage.

        Args:
            message (VideoMessage): The video message to be ingested
            source (str): The source SQS queue

        Returns:
            bool: True if it is ingestable, False otherwise
        """
        if not message.validate():
            LOGGER.info("Message deemed invalid for ingestion, ignoring",
                        extra={"messageid": message.messageid})
            self.__delete_message(message, source)
            return False

        if message.is_irrelevant(self.config.tenant_blacklist, self.config.recorder_blacklist):
            LOGGER.info("Message deemed irrelevant", extra={"messageid": message.messageid})
            self.__delete_message(message, source)
            return False

        return True

    def handle_interior_recorder(self, message: dict, source: str) -> None:
        """ Forwards an INTERIOR message to a VideoIngestor if the message is ingestible.

        Args:
            message (dict): The video interior message to be ingested
            source (str): The source SQS queue
        """
        video_message = VideoMessage(message)

        if not self.message_ingestable(video_message, source):
            return

        # ask for the request of training data
        self.__send_to_selector(video_message)

        metadata_is_complete, metadata_chunks = self.metadata_ing.check_allparts_exist(
            video_message)  # pylint disable=line-too-long
        if not metadata_is_complete:
            LOGGER.info("Metadata not available yet")
            self.__increase_message_visability_timeout(video_message, source)
            return

        db_record_data: Optional[dict] = self.video_ing.ingest(
            video_message, self.config.training_whitelist, self.config.request_training_upload)
        if not db_record_data:
            LOGGER.error("Fail to ingest video")
            self.__increase_message_visability_timeout(video_message, source)
            return

        is_metadata_ingested = self.metadata_ing.ingest(video_message, db_record_data['_id'], metadata_chunks)
        if not is_metadata_ingested:
            LOGGER.error("Some error ocurred while attempting to ingest metadata")
            self.__increase_message_visability_timeout(video_message, source)
            return

        if db_record_data:
            db_record_data.update({"MDF_available": "Yes", "sync_file_ext": METADATA_FILE_EXT})
            self.__send_to_metadata(video_message, db_record_data)
            self.__delete_message(video_message, source)

    def handle_training_recorder(self, message: dict, source: str) -> None:
        """ Forwards a TRAINING message to a VideoIngestor if the message is ingestible.

        Args:
            message (dict): The video training message to be ingested
            source (str): The source SQS queue
        """
        video_message = VideoMessage(message)

        if not self.message_ingestable(video_message, source):
            return

        imu_is_complete, imu_chunks = self.imu_ing.check_allparts_exist(video_message)
        if not imu_is_complete:
            LOGGER.warning("IMU not available yet")
            self.__increase_message_visability_timeout(video_message, source)
            return

        db_record_data: Optional[dict] = self.video_ing.ingest(
            video_message, self.config.training_whitelist, self.config.request_training_upload)
        if not db_record_data:
            LOGGER.error("Fail to ingest video")
            self.__increase_message_visability_timeout(video_message, source)
            return

        imu_path = self.imu_ing.ingest(video_message, db_record_data['_id'], imu_chunks)
        if not imu_path:
            LOGGER.error("Some error ocurred while attempting to ingest IMU data")
            self.__increase_message_visability_timeout(video_message, source)
            return

        if db_record_data:
            db_record_data.update({"imu_path": imu_path})
            self.__send_to_metadata(video_message, db_record_data)
            self.__delete_message(video_message, source)

    def handle_snapshot(self, message: dict, source: str) -> None:
        """ Forwards a TRAINING_MULTI_SNAPSHOT message to a SnapshotIngestor if the message is ingestible.

        Args:
            message (dict): The video training message to be ingested
            source (str): The source SQS queue
        """
        snap_msg_obj = SnapshotMessage(message)

        if not snap_msg_obj.validate():
            LOGGER.info("Message deemed invalid for ingestion, ignoring",
                        extra={"messageid": snap_msg_obj.messageid})
            self.__delete_message(snap_msg_obj, source)
            return

        if snap_msg_obj.is_irrelevant(self.config.tenant_blacklist):
            LOGGER.info("Message deemed irrelevant", extra={"messageid": snap_msg_obj.messageid})
            self.__delete_message(snap_msg_obj, source)
            return

        data: Optional[dict] = self.snap_ing.ingest(snap_msg_obj)
        if not data:
            LOGGER.warning("Some error ocurred while attempting to ingest snapshots")
            self.__increase_message_visability_timeout(snap_msg_obj, source)

    def route(self, message: dict, source: str) -> None:
        """ Routes a message to its correct handler for processing.

        Args:
            message (dict): The video training message to be ingested
            source (str): The source SQS queue
        """
        message_type = IngestorHandler.message_type_identifier(message)
        LOGGER.info("Pulled a message from %s (%s) -> %s ", source, message_type,
                    message, extra={"messageid": message.get('MessageId')})

        if message_type == TRAINING_RECORDER:
            self.handle_training_recorder(message, source)
        elif message_type == INTERIOR_RECORDER:
            self.handle_interior_recorder(message, source)
        elif message_type == FRONT_RECORDER:
            LOGGER.info("Received a FrontRecording, ignoring it")
            self.cont_services.delete_message(self.sqs_client, message.get("ReceiptHandle"), source)
            # this is to be replaced with a __delete_message()
            # when we create a FrontIngestor and FrontMessage
        elif message_type == SNAPSHOT:
            self.handle_snapshot(message, source)
        else:
            LOGGER.error(
                "Could not identify message type as video nor snapshot related, ignoring",
                extra={"messageid": message.get('MessageId')})

    def __delete_message(self, message: Message, source: str) -> None:
        """ Deletes the message from the queue.

        Args:
            message (Message): Message to be deleted.
            source (str): The source to be deleted from.
        """
        self.cont_services.delete_message(self.sqs_client, message.receipthandle, source)
        LOGGER.info("Message deleted from %s", source, extra={"messageid": message.messageid})

    def __send_to_metadata(self, message: Message, data: dict) -> None:
        """ Sends an update to the DB by using the metadata queue.

        Args:
            message (Message): Message to be deleted.
            source (str): The source to be deleted from.
        """
        self.cont_services.send_message(self.sqs_client, self.metadata_queue, data)
        LOGGER.info("Message sent to %s", self.metadata_queue,
                    extra={"messageid": message.messageid})

    def __send_to_selector(self, message: VideoMessage) -> None:
        """ Notifies Selector of the arrival of a video.

        Args:
            message (VideoMessage): message of the ingested video.
        """
        data = {
            "streamName": message.streamname,
            "deviceId": message.deviceid,
            "footageFrom": message.footagefrom,
            "footageTo": message.footageto
        }
        self.cont_services.send_message(self.sqs_client, self.hq_request_queue, data)
        LOGGER.info("Message sent to %s", self.hq_request_queue,
                    extra={"messageid": message.messageid})

    def __increase_message_visability_timeout(self, message_obj: Message, source: str) -> None:
        """ Increases the message visibility timeout.

        Args:
            message (Message): Message to be deleted.
            source (str): The source to be deleted from.
        """
        prolong_time = MESSAGE_VISIBILITY_EXTENSION_HOURS[min(
            message_obj.receive_count, len(MESSAGE_VISIBILITY_EXTENSION_HOURS) - 1)] * 3600
        LOGGER.warning("Prolonging message visibility timeout for %d seconds",
                       prolong_time)
        self.cont_services.update_message_visibility(self.sqs_client, message_obj.receipthandle,
                                                     prolong_time, source)


def main(config: SDRetrieverConfig):
    """Main function of the component.

    Args:
        config (SDRetrieverConfig): configmap of the component
    """

    # Define configuration for logging messages
    LOGGER.info("Starting Container %s %s", CONTAINER_NAME, CONTAINER_VERSION)

    # Start necessary services
    s3_client = boto3.client('s3', region_name='eu-central-1')
    sqs_client = boto3.client('sqs', region_name='eu-central-1')
    sts_client = boto3.client('sts', region_name='eu-central-1')
    cont_services = ContainerServices(container=CONTAINER_NAME, version=CONTAINER_VERSION)
    cont_services.load_config_vars()
    sts_helper = StsHelper(sts_client, role=cont_services.rcc_info.get("role"), role_session="DevCloud-SDRetriever")
    graceful_exit = GracefulExit()

    # Create file ingestors
    metadata_ing = MetadataIngestor(
        container_services=cont_services,
        s3_client=s3_client,
        sqs_client=sqs_client,
        sts_helper=sts_helper)
    snapshot_ing = SnapshotIngestor(
        container_services=cont_services,
        s3_client=s3_client,
        sqs_client=sqs_client,
        sts_helper=sts_helper)
    video_ing = VideoIngestor(
        container_services=cont_services,
        s3_client=s3_client,
        sqs_client=sqs_client,
        sts_helper=sts_helper,
        frame_buffer=config.frame_buffer)
    imu_ingestor = IMUIngestor(
        container_services=cont_services,
        s3_client=s3_client,
        sqs_client=sqs_client,
        sts_helper=sts_helper)

    ingestion_handler = IngestorHandler(
        imu_ingestor,
        metadata_ing,
        video_ing,
        snapshot_ing,
        cont_services,
        config,
        sqs_client)

    # Create source commuter
    src = SourceCommuter([
        cont_services.sqs_queues_list['SDRetriever'],
        cont_services.sqs_queues_list['Selector']])
    while graceful_exit.continue_running:
        # Poll source (SQS queue) for a new message
        source = src.get_source()
        message = cont_services.get_single_message_from_input_queue(sqs_client, source)

        if message:
            ingestion_handler.route(message, source)
        else:
            # if no message was obtained from the current source
            src.next()

    LOGGER.info('%s exited gracefully.', CONTAINER_NAME)


if __name__ == "__main__":
    _config = SDRetrieverConfig.load_config_from_yaml_file(
        os.environ.get('CONFIG_FILE', '/app/config/config.yml'))

    # Instanciating main loop and injecting dependencies
    main(config=_config)