from requests import Request
from base.model.artifacts import AnonymizationResult
from artifact_downloader.exceptions import UnexpectedContainerMessage
from artifact_downloader.message.incoming_messages import AnonymizeMessage
from artifact_downloader.container_handlers.handler import ContainerHandler

class AnonymizeContainerHandler(ContainerHandler):

    def create_request(self, message: AnonymizeMessage) -> Request:
        if not isinstance(AnonymizationResult, message.body):
            raise UnexpectedContainerMessage(f"Message of type {type(message.body)} is not an anonymize message")

        return self.__handle_anon_result(message.body)


    def __handle_anon_result(self, message: AnonymizationResult) -> Request:
        pass
