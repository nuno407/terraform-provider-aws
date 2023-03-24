"""Sensor Data Retriever - V7
- adds ingestion of TrainingRecorder IMU data
"""

import logging as log
import os

import boto3
from base import GracefulExit
from base.aws.container_services import ContainerServices
from base.aws.shared_functions import StsHelper
from sdretriever.config import SDRetrieverConfig

from sdretriever.ingestor.imu import IMUIngestor
from sdretriever.ingestor.metadata import MetadataIngestor
from sdretriever.ingestor.snapshot import SnapshotIngestor
from sdretriever.ingestor.video import VideoIngestor
from sdretriever.sourcecommuter import SourceCommuter
from sdretriever.handler import IngestorHandler
from sdretriever.constants import CONTAINER_NAME, CONTAINER_VERSION

# Global log message formatting
LOGGER = log.getLogger("SDRetriever")
ContainerServices.configure_logging("SDRetriever")


def main(config: SDRetrieverConfig):
    """Main function of the component.

    Args:
        config (SDRetrieverConfig): configmap of the component
    """

    # Define configuration for logging messages
    LOGGER.info("Starting Container %s %s", CONTAINER_NAME, CONTAINER_VERSION)

    # Start necessary services
    s3_client = boto3.client("s3", region_name="eu-central-1")
    sqs_client = boto3.client("sqs", region_name="eu-central-1")
    sts_client = boto3.client("sts", region_name="eu-central-1")
    cont_services = ContainerServices(container=CONTAINER_NAME, version=CONTAINER_VERSION)
    cont_services.load_config_vars()
    sts_helper = StsHelper(sts_client,
                           role=cont_services.rcc_info.get("role"),
                           role_session="DevCloud-SDRetriever")
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
        frame_buffer=config.frame_buffer,
        discard_repeated_video=config.discard_video_already_ingested)
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
        cont_services.sqs_queues_list["SDRetriever"],
        cont_services.sqs_queues_list["Selector"]])
    while graceful_exit.continue_running:
        # Poll source (SQS queue) for a new message
        source = src.get_source()
        message = cont_services.get_single_message_from_input_queue(sqs_client, source)

        if message:
            ingestion_handler.route(message, source)
        else:
            # if no message was obtained from the current source
            src.next()

    LOGGER.info("%s exited gracefully.", CONTAINER_NAME)


if __name__ == "__main__":
    _config = SDRetrieverConfig.load_config_from_yaml_file(
        os.environ.get("CONFIG_FILE", "/app/config/config.yml"))

    # Instanciating main loop and injecting dependencies
    main(config=_config)
