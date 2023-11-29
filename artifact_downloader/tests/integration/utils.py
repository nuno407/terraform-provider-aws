import os
import json
from base.testing.utils import load_relative_json_file


def load_relative_sqs_message(filename: str) -> dict:
    data = load_relative_json_file(__file__, os.path.join("data", "sqs_messages", filename))
    if isinstance(data["Body"], dict):
        data["Body"] = json.dumps(data["Body"])
    return data


def load_relative_post_data(filename: str) -> dict:
    return load_relative_json_file(__file__, os.path.join("data", "post_data", filename))
