from requests import Request
from base.model.artifacts import OperatorArtifact, EventArtifact
from artifact_downloader.exceptions import UnexpectedContainerMessage
from artifact_downloader.message.incoming_messages import MDFParserMessage
from artifact_downloader.container_handlers.handler import ContainerHandler

class SanitizerContainerHandler(ContainerHandler):

    def create_request(self, message: MDFParserMessage) -> Request:
        if isinstance(OperatorArtifact, message.body):
            return self.__handle_operator_artifact(message.body)
        elif  isinstance(EventArtifact, message.body):
            return self.__handle_event_artifact(message.body)

        raise UnexpectedContainerMessage(f"Message of type {type(message.body)} is not a sanitizer message")


    def __handle_event_artifact(self, message: EventArtifact) -> Request:
        pass

    def __handle_operator_artifact(self, message: OperatorArtifact) -> Request:
        pass
