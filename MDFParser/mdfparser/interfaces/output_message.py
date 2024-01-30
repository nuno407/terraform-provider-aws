"""OutputMessage Class"""
import json
from dataclasses import asdict, dataclass
from typing import Union

from mdfparser.interfaces.input_message import DataType


@dataclass
class OutputMessage():
    """
    Input message
    All the messages sent to the Metadata queue should have this structure.
    """
    _id: str
    parsed_file_path: str
    data_type: DataType
    recording_overview: dict[str, Union[float, int]]
    tenant: str
    raw_s3_path: str

    def to_json(self) -> str:
        """
        Serializes the OutputMessage

        Returns:
            str: A serialized json.
        """
        dic = asdict(self)
        dic["data_type"] = self.data_type.value
        return json.dumps(dic)
