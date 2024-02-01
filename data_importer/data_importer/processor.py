"""Processor Module"""
import abc
from abc import ABC
from typing import Any, Optional

from data_importer.sqs_message import SQSMessage

# pylint: disable=too-few-public-methods


class Processor(ABC):
    """Abstract Processor provides a method to load metadata"""

    @classmethod
    @abc.abstractmethod
    def process(cls, message: SQSMessage, **_kwargs: Any) -> Optional[dict[str, Any]]:
        """Process a SQSMessage."""
