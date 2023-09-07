# type: ignore
# pylint: disable=too-few-public-methods, line-too-long
"""Snapshot artifact module."""
import logging

from kink import inject

from base.model.artifacts import OperatorArtifact
from healthcheck.controller.db import DatabaseController
from healthcheck.database import DBCollection

_logger: logging.Logger = logging.getLogger(__name__)


@inject()
class SAVOperatorArtifactChecker:
    """Operator artifact checker."""

    def __init__(
            self,
            db_controller: DatabaseController
    ):
        self.__db_controller = db_controller

    def run_healthcheck(self, artifact: OperatorArtifact) -> None:
        """
        Run healthcheck

        Args:
            artifact (Artifact): data ingestion artifact
        """
        _logger.info("running healthcheck for SAV Operator")

        # Check if artifact is in database
        self.__db_controller.is_operator_feedback_present_or_raise(artifact, DBCollection.SAV_OPERATOR_FEEDBACK)
