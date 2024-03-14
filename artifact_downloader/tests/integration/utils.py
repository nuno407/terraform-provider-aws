import io
import os
import pandas as pd
import json
from artifact_downloader.memory_buffer import UnclosableMemoryBuffer
from base.testing.utils import load_relative_json_file


def load_relative_sqs_message(filename: str) -> dict:
    data = load_relative_json_file(__file__, os.path.join("data", "sqs_messages", filename))
    if isinstance(data["Body"], dict):
        data["Body"] = json.dumps(data["Body"])
    return data


def load_relative_post_data(filename: str) -> dict:
    return load_relative_json_file(__file__, os.path.join("data", "post_data", filename))


def transform_imu_json_bytes_to_parquet(data: bytes) -> UnclosableMemoryBuffer:
    buffer_read = io.BytesIO(data)
    df = pd.read_json(buffer_read)
    buffer_write = UnclosableMemoryBuffer()
    df.to_parquet(buffer_write, engine="fastparquet", index=False)
    return buffer_write
