from dataclasses import dataclass
from datetime import datetime
from base.aws.container_services import ContainerServices
from healthcheck.rcc_artifact_checker import RCCArtifact


class RCCHealthCheck:
    """
    This is specific to RCC.
    In the future we might have the need to handle a new cloud.
    """

    @dataclass
    class ParsedMessage:
        artifact_type: int  # Either VIDEO_TYPE or VIDEO_TYPE
        artifact_id: str
        footage_from: datetime = datetime.min
        footage_to: datetime = datetime.min

    def __init__(self, container_services: ContainerServices, s3_client, sqs_client):
        ...

    def get_info_from_rcc_queue_message(self, message: dict) -> ParsedMessage:
        """
        Parses and retrieves the ID of the message recieved from the RCC.
        Remarks:
        The message should already be parsed. (Without the json in string format)

        Args:
            message (dict): Message retrieved from the queue downoad comming from the RCC.

        Return:
            ParsedMessage: Contains information about the class
        """
        ...

    def get_info_from_custom_queue_message(self, message: dict) -> ParsedMessage:
        """
        Parses and retrieves the ID of the message recieved inputed manually in the queue

        Args:
            message (dict): Message retrieved from the queue download manually inputed.

        Return:
            ParsedMessage: Contains information about the class
        """
        ...

    def get_artifact_from_message(self, message: ParsedMessage) -> RCCArtifact:
        ...

    def on_check_fail(self, error_message: str, message: dict) -> None:
        ...

    def on_check_success(self, message: dict) -> None:
        ...

    def on_message_not_ingested(self, message: dict) -> None:
        ...

    def run(self) -> None:
        """
        Main loop
        This should be a non-blocking function
        """
