"""SQS Message module"""

import json
import os
from dataclasses import dataclass, field
from pathlib import Path

from base.aws.container_services import ContainerServices

_logger = ContainerServices.configure_logging(__name__)


@dataclass
class SQSMessage():
    """
    Class holing information of a parsed SQS message
    """

    principal_id: str
    bucket_name: str
    file_path: str
    file_extension: str
    dataset: str
    full_path: str = field(init=False)

    def __post_init__(self):
        self.full_path = f"s3://{self.bucket_name}/{self.file_path}"

    # Logs the message
    def print_message(self):
        """
        Print message attributes to log.
        """
        _logger.debug("Message")
        _logger.debug("principal_id: %s", self.principal_id)
        _logger.debug("bucket_name: %s", self.bucket_name)
        _logger.debug("file_path: %s", self.file_path)
        _logger.debug("file_extension: %s", self.file_extension)
        _logger.debug("dataset: %s", self.dataset)
        _logger.debug("full_path: %s", self.full_path)

    @classmethod
    def from_raw_sqs_message(cls, sqs_message):
        """
        Creates an SQS Message object from parsing a raw message

        :param sqs_message: Raw message to parse
        :return: parsed SQSMessage
        """
        body = sqs_message["Body"].replace("\'", "\"")
        sqs_body = json.loads(body)

        principal_id = sqs_body["Records"][0]["userIdentity"]["principalId"]

        bucket_name = sqs_body["Records"][0]["s3"]["bucket"]["name"]
        file_path = Path(sqs_body["Records"][0]["s3"]["object"]["key"])
        # assumed that file extension is always given (S3 notification configuration)
        file_extension = file_path.suffix.strip(".").lower()

        (directory_structure, _) = os.path.split(file_path)
        dir_splits = str.split(directory_structure, "/")
        dataset = dir_splits[0] if dir_splits[0] != "" else "default"

        message_to_return = SQSMessage(principal_id, bucket_name, str(file_path), file_extension, dataset)
        message_to_return.print_message()

        return message_to_return
