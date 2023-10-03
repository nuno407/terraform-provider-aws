from requests import Request
from base.model.artifacts import IMUProcessingResult, SignalsProcessingResult
from artifact_downloader.exceptions import UnexpectedContainerMessage
from artifact_downloader.message.incoming_messages import MDFParserMessage
from artifact_downloader.container_handlers.handler import ContainerHandler

class MDFParserContainerHandler(ContainerHandler):

    def create_request(self, message: MDFParserMessage) -> Request:
        if isinstance(SignalsProcessingResult, message.body):
            return self.__handle_signals_result(message.body)
        elif  isinstance(IMUProcessingResult, message.body):
            return self.__handle_imu_result(message.body)

        raise UnexpectedContainerMessage(f"Message of type {type(message.body)} is not a MDF message")


    def __handle_imu_result(self, message: IMUProcessingResult) -> Request:
        pass

    def __handle_signals_result(self, message: SignalsProcessingResult) -> Request:
        pass
