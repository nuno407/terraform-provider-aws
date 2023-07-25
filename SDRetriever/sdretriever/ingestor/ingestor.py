""" ingestor module. """
import logging as log
from abc import abstractmethod

from kink import inject

from base.model.artifacts import Artifact


@inject
class Ingestor:
    """Ingestor base class"""

    @abstractmethod
    def ingest(self, artifact: Artifact):
        """Ingests the artifacts described in a message into the DevCloud"""
