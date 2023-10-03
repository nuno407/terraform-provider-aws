from requests import Request
from base.model.artifacts import ProcessingStep
from artifact_downloader.exceptions import UnexpectedContainerMessage
from artifact_downloader.message.incoming_messages import SDMMessage
from artifact_downloader.container_handlers.handler import ContainerHandler

class SDMContainerHandler(ContainerHandler):

    def create_request(self, message: SDMMessage) -> Request:
        if not isinstance(ProcessingStep, message.body):
            raise UnexpectedContainerMessage(f"Message of type {type(message.body)} is not a SDM message")

        return self.__handle_sdm_artifact(message.body)


    def __handle_sdm_artifact(self, message: ProcessingStep) -> Request:
        pass
