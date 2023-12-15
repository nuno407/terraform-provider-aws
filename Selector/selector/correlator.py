"""File with class responsible for correlating rules in db and sending them to metadata queue"""
import logging
from kink import inject

from base.aws.sqs import SQSController
from base.model.artifacts import (S3VideoArtifact, SnapshotArtifact,
                                  VideoUploadRule, SnapshotUploadRule,
                                  SelectorRule, DEFAULT_RULE)
from selector.constants import CONTAINER_NAME
from selector.model import DBDecision

logger = logging.getLogger(__name__)


@inject
class Correlator:
    """Class responsible for correlating rules in db and sending them to metadata queue"""

    def __init__(self, sqs_metadata_controller: SQSController):
        self.__sqs_controller = sqs_metadata_controller

    def correlate_video_rules(self, video_artifact: S3VideoArtifact) -> bool:
        """When a S3VideoArtifact is ingested, find DBDecisions in the DB with
           the same footage_id. If there are matches, create new VideoUploadRules,
           and send them to Metadata service to be processed.

        Args:
            video_artifact (VideoArtifact): S3VideoArtifact from SDR

        Returns:
            bool: success of the operation
        """

        rule_list: list[VideoUploadRule] = []

        try:
            for decision in DBDecision.objects(footage_id=video_artifact.footage_id):  # pylint: disable=no-member
                rule_list.append(VideoUploadRule(
                    tenant=video_artifact.tenant_id,
                    raw_file_path=video_artifact.raw_s3_path,
                    video_id=video_artifact.artifact_id,
                    footage_from=decision.footage_from,
                    footage_to=decision.footage_to,
                    rule=SelectorRule(
                        rule_name=decision.rule_name,
                        rule_version=decision.rule_version,
                        origin=decision.origin
                    )
                ))

            if len(rule_list) == 0:
                logger.info("No match found for Training Video, setting default rule.")
                rule_list.append(VideoUploadRule(
                    tenant=video_artifact.tenant_id,
                    raw_file_path=video_artifact.raw_s3_path,
                    video_id=video_artifact.artifact_id,
                    footage_from=video_artifact.timestamp,
                    footage_to=video_artifact.end_timestamp,
                    rule=DEFAULT_RULE
                ))

            for rule_artifact in rule_list:
                raw_message = rule_artifact.stringify()

                self.__sqs_controller.send_message(raw_message,
                                                   CONTAINER_NAME)

        except Exception as error:  # pylint: disable=broad-except
            logger.error("Unexpected error occured when processing Training Video rules: %s", error)
            return False

        logger.info("Processing successful with %d video upload rule(s) sent to metadata queue", len(rule_list))
        return True

    def correlate_snapshot_rules(self, artifact: SnapshotArtifact) -> bool:
        """When a SnapshotArtifact is ingested, send a default rule to Metadata. This is
           the workflow for now, as no snapshot id is saved to the DB.

           In the future, this method should find DBDecisions in the DB with
           the same id. If there are matches, create new SnapshotUploadRule,
           and send it to Metadata service to be processed.

        Args:
            artifact (SnapshotArtifact): SnapshotArtifact from SDR

        Returns:
            bool: success of the operation
        """
        try:
            rule_artifact = SnapshotUploadRule(
                tenant=artifact.tenant_id,
                raw_file_path=artifact.raw_s3_path,
                snapshot_id=artifact.artifact_id,
                snapshot_timestamp=artifact.timestamp,
                rule=DEFAULT_RULE
            )

            raw_snap_message = rule_artifact.stringify()
            self.__sqs_controller.send_message(raw_snap_message,
                                               CONTAINER_NAME)
        except Exception as error:  # pylint: disable=broad-except
            logger.error("Unexpected error occured when processing Snapshot rules: %s", error)
            return False

        logger.info("Snapshot Upload Rule message sent to metadata queue")
        return True
