""" ingestion handler module. """
import logging as log
from typing import Iterator

from kink import inject
from mypy_boto3_sqs.type_defs import MessageTypeDef

from base.aws.container_services import ContainerServices
from base.aws.sqs import SQSController
from base.model.artifacts import (Artifact, IMUArtifact, KinesisVideoArtifact,
                                  RecorderType, S3VideoArtifact,
                                  SignalsArtifact, SnapshotArtifact,
                                  VideoArtifact)
from base.model.artifacts import (Artifact, IMUArtifact,
                                  RecorderType, SignalsArtifact,
                                  SnapshotArtifact, VideoArtifact, PreviewSignalsArtifact)
from sdretriever.config import SDRetrieverConfig
from sdretriever.constants import (CONTAINER_NAME,
                                   MESSAGE_VISIBILITY_EXTENSION_HOURS)
from sdretriever.exceptions import (FileAlreadyExists,
                                    NoIngestorForArtifactError,
                                    TemporaryIngestionError)
from sdretriever.ingestor.imu import IMUIngestor
from sdretriever.ingestor.ingestor import Ingestor
from sdretriever.ingestor.kinesis_video import KinesisVideoIngestor
from sdretriever.ingestor.s3_video import S3VideoIngestor
from sdretriever.ingestor.snapshot import SnapshotIngestor
from sdretriever.ingestor.snapshot_metadata import SnapshotMetadataIngestor
from sdretriever.ingestor.preview_metadata import PreviewMetadataIngestor
from sdretriever.ingestor.video_metadata import VideoMetadataIngestor

_logger = log.getLogger("SDRetriever")


@inject
class IngestionHandler:  # pylint: disable=too-many-instance-attributes, too-few-public-methods
    """Handler logic for SDR ingestion scenarios
    """

    def __init__(  # pylint: disable=too-many-arguments
            self,
            imu_ing: IMUIngestor,
            metadata_ing: VideoMetadataIngestor,
            kinesis_video_ing: KinesisVideoIngestor,
            s3_video_ing: S3VideoIngestor,
            snap_ing: SnapshotIngestor,
            snap_metadata_ing: SnapshotMetadataIngestor,
            preview_metadata_ing: PreviewMetadataIngestor,
            cont_services: ContainerServices,
            config: SDRetrieverConfig,
            sqs_controller: SQSController):
        """IngestorHandler init

        Args:
            imu_ing (IMUIngestor): instance of IMUIngestor
            metadata_ing (MetadataIngestor): instance of MetadataIngestor
            video_ing (KinesisVideoIngestor): instance of KinesisVideoIngestor
            snap_ing (SnapshotIngestor): instance of SnapshotIngestor
            cont_services (ContainerServices): instance of ContainerServices
            config (SDRetrieverConfig): instance of SDRetrieverConfig
            sqs_controller (SQSController): instance of SQS client
        """
        self.__video_metadata_ing = metadata_ing
        self.__kinesis_video_ing = kinesis_video_ing
        self.__s3_video_ing = s3_video_ing
        self.__sqs_controller = sqs_controller
        self.__config = config
        self.__imu_ing = imu_ing
        self.__snap_ing = snap_ing
        self.__preview_metadata_ing = preview_metadata_ing
        self.__snap_metadata_ing = snap_metadata_ing
        self.__metadata_queue = cont_services.sqs_queues_list["Metadata"]
        self.__selector_queue = cont_services.sqs_queues_list["Selector"]
        self.__mdfp_queue = cont_services.sqs_queues_list["MDFParser"]


    def _get_ingestor(self, artifact: Artifact) -> Ingestor:
        """ Returns the ingestor that should handle the artifact.

        Args:
            artifact (Artifact): The artifact to be processed

        Returns:
            Optional[Ingestor]: The ingestor that should handle the artifact
        """
        if isinstance(artifact, KinesisVideoArtifact):
            return self.__kinesis_video_ing
        if isinstance(artifact, S3VideoArtifact):
            return self.__s3_video_ing
        if isinstance(artifact, SnapshotArtifact):
            return self.__snap_ing
        if isinstance(
                artifact,
                SignalsArtifact) and isinstance(
                artifact.referred_artifact,
                VideoArtifact):
            return self.__video_metadata_ing
        if isinstance(
                artifact,
                SignalsArtifact) and isinstance(
                artifact.referred_artifact,
                SnapshotArtifact):
            return self.__snap_metadata_ing
        if isinstance(artifact, IMUArtifact):
            return self.__imu_ing

        if isinstance(artifact, PreviewSignalsArtifact):
            return self.__preview_metadata_ing

        raise NoIngestorForArtifactError()

    def _get_forward_queues(self, artifact: Artifact) -> Iterator[str]:
        """ Gets the queues that the artifact should be forwarded to."""

        if isinstance(artifact, IMUArtifact):
            yield self.__mdfp_queue

        if isinstance(artifact, SignalsArtifact) and \
                isinstance(artifact.referred_artifact, VideoArtifact):
            yield self.__mdfp_queue

        if isinstance(artifact, SignalsArtifact) and \
                isinstance(artifact.referred_artifact, SnapshotArtifact):
            yield self.__metadata_queue

        if isinstance(artifact, (VideoArtifact, SnapshotArtifact)):
            yield self.__metadata_queue

        if isinstance(artifact, PreviewSignalsArtifact):
            yield self.__selector_queue

        if (isinstance(artifact, VideoArtifact) and
            artifact.recorder == RecorderType.INTERIOR and
                self.__config.request_training_upload):
            yield self.__selector_queue

    def handle(self, artifact: Artifact, message: MessageTypeDef) -> None:
        """ Routes a message to its correct handler for processing.

        Args:
            artifact (Artifact): The artifact to be processed
            message (MessageTypeDef): The message received from the SQS queue
        """
        _logger.info("Received a %s artifact: %s", type(
            artifact).__name__, artifact.artifact_id)

        try:
            # ingest artifact
            ingestor = self._get_ingestor(artifact)
            ingestor.ingest(artifact)

            # forward artifact to next queue(s)
            queues = list(self._get_forward_queues(artifact))
            self.__send_to_queues(artifact, queues)
            self.__sqs_controller.delete_message(message)

        except TemporaryIngestionError as excpt:
            _logger.exception(excpt)
            self.__increase_message_visability_timeout(message)
        except (FileAlreadyExists, NoIngestorForArtifactError) as excpt:
            _logger.info(str(excpt))
            self.__sqs_controller.delete_message(message)

    def __send_to_queues(self, artifact: Artifact, queues: list[str]) -> None:
        """ Sends an update to the desired queues.

        Args:
            artifact (Artifact): The artifact to be sent to the metadata queue
            queues (List[str]): The queues to send the artifact to
        """
        raw_message = artifact.stringify()
        for queue in queues:
            self.__sqs_controller.send_message(
                raw_message, CONTAINER_NAME, queue)
            _logger.info("Message sent to %s", queue)

    def __increase_message_visability_timeout(self, message_obj: MessageTypeDef) -> None:
        """ Increases the message visibility timeout.

        Args:
            message (Message): Message to be deleted.
            source (str): The source to be deleted from.
        """
        factor_idx = int(message_obj["Attributes"]["ApproximateReceiveCount"])-1
        factor_idx = min(factor_idx,len(MESSAGE_VISIBILITY_EXTENSION_HOURS) - 1)
        factor_idx = max(factor_idx,0)
        prolong_time = int(
            MESSAGE_VISIBILITY_EXTENSION_HOURS[factor_idx] * 3600)  # type: ignore
        _logger.warning("Prolonging message visibility timeout for %d seconds",
                        prolong_time)
        self.__sqs_controller.try_update_message_visibility_timeout(
            message_obj, prolong_time)