"""General Handler Class"""
from abc import ABC, abstractmethod
from typing import Optional

from mdfparser.interfaces.input_message import DataType, InputMessage
from mdfparser.interfaces.output_message import OutputMessage


class Handler(ABC):
    """
    This abstract class shall be implemented by every Handler that needs to process a message.
    It then needs to be Initilized on the constructor of the Consumer.
    """
    @abstractmethod
    def ingest(self, message: InputMessage) -> Optional[OutputMessage]:
        """
        The function will be called by the consumer if the data_type of the InputMessage
        matches the one returned by the handler_type.

        Args:
            message (InputMessage): The Message parsed from the SQS.

        Returns:
            Optional[OutputMessage]: If an OutputMessage is returned, it will be sent to the Metadata service.
        """
        raise NotImplementedError("Ingest not implemented")

    @abstractmethod
    def handler_type(self) -> DataType:
        """
        This shall return the data type that should be process by the handler.

        Remarks:
        Is important that it exists ONLY one DataType per handler!

        Returns:
            DataType: The Datatype to be processed by this handler.
        """
        raise NotImplementedError("Handler_type not implemented")
