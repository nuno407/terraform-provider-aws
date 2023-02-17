import abc
from abc import ABC
from typing import Any, Optional

from data_importer.sqs_message import SQSMessage


class Processor(ABC):
    @classmethod
    @abc.abstractmethod
    def load_metadata(cls, message: SQSMessage, **kwargs) -> Optional[dict[str, Any]]:
        raise NotImplementedError("This method should be implemented by subclasses.")
