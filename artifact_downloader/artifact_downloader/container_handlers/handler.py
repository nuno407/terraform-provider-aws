from abc import abstractmethod
from requests import Request
from artifact_downloader.message.incoming_messages import SqsMessage


class ContainerHandler:

    @abstractmethod
    def create_request(self, message: SqsMessage) -> Request:
        raise NotImplementedError

