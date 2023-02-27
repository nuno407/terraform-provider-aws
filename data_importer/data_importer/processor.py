"""Processor Module"""
import abc
from abc import ABC
from typing import Any, Optional
from fiftyone import Dataset

from data_importer.fiftyone_importer import FiftyoneImporter
from data_importer.sqs_message import SQSMessage


# pylint: disable=too-few-public-methods


class Processor(ABC):
    """Abstract Processor provides a method to load metadata"""

    @classmethod
    @abc.abstractmethod
    def load_metadata(cls, message: SQSMessage, **kwargs) -> Optional[dict[str, Any]]:
        """Load metadata for this message"""

    @classmethod
    @abc.abstractmethod
    def upsert_sample(cls, dataset: Dataset, message: SQSMessage, metadata: dict[str, Any],
                      importer: FiftyoneImporter) -> Any:
        """Store metadata for this message"""
