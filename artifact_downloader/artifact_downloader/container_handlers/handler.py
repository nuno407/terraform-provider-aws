"""Interface for container handlers"""
from abc import abstractmethod
from requests import Request
from artifact_downloader.message.incoming_messages import SqsMessage


class ContainerHandler:  # pylint: disable=too-few-public-methods
    """Interface for container handlers"""

    @abstractmethod
    def create_request(self, message: SqsMessage) -> Request:
        """
        Abstract method that should return the needed request

        Args:
            message (SqsMessage): The SQSMessage that will be passed

        Raises:
            NotImplementedError: Needs to be implemented by the derived class

        Returns:
            Request: The request to be made to the HTTP Client
        """
        raise NotImplementedError
