from requests import Request
from base.model.artifacts import CHCResult
from artifact_downloader.exceptions import UnexpectedContainerMessage
from artifact_downloader.message.incoming_messages import CHCMessage
from artifact_downloader.container_handlers.handler import ContainerHandler

class CHCContainerHandler(ContainerHandler):

    def create_request(self, message: CHCMessage) -> Request:
        if not isinstance(CHCResult, message.body):
            raise UnexpectedContainerMessage(f"Message of type {type(message.body)} is not chc message")

        return self.__handle_chc_result(message.body)


    def __handle_chc_result(self, message: CHCResult) -> Request:
        pass
