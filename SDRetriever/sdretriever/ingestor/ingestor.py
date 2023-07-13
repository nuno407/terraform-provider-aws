""" ingestor module. """
import logging as log
from abc import abstractmethod

from kink import inject

from base.aws.s3 import S3ClientFactory
from base.model.artifacts import Artifact

_logger = log.getLogger("SDRetriever." + __name__)


@inject
class Ingestor:
    """Ingestor base class"""

    @abstractmethod
    def ingest(self, artifact: Artifact):
        """Ingests the artifacts described in a message into the DevCloud"""
