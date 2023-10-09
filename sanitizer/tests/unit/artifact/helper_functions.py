import os
from base.aws.model import SQSMessage
from base.testing.utils import load_relative_json_file


def load_sqs_json(fixture_file_id: str) -> dict[str, str]:
    return load_relative_json_file(__file__, os.path.join("..", "data", "message_parser", fixture_file_id))


def parse_sqs_message(fixture_file_id: str):
    message = load_sqs_json(fixture_file_id)
    return SQSMessage(message_id=message["MessageId"],
                      receipt_handle=message["ReceiptHandle"],
                      body=message["Body"],
                      timestamp=message["Body"]["Timestamp"],
                      attributes=message["Attributes"])
