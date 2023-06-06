"""Utility functions to be used on tests"""
import os
from typing import Any
import json


def get_abs_path(caller_file: str, file_path: str) -> str:
    """

    Args:
        caller_file (str): Should be called with __file__ method
        filename (str): The file path to load

    Returns:
        str: THe absolute path to the file
    """
    return os.path.join(os.path.dirname(os.path.abspath(caller_file)),file_path)

def load_relative_raw_file(caller_file: str, file_path: str) -> bytes:
    """

    Args:
        caller_file (str): Should be called with __file__ method
        filename (str): The file path to load

    Returns:
        bytes: _description_
    """
    path_to_file = get_abs_path(caller_file, file_path)
    with open(path_to_file, "rb") as file:
        return file.read()

def load_relative_str_file(caller_file: str, file_path: str) -> bytes:
    """

    Args:
        caller_file (str): Should be called with __file__ method
        filename (str): The file path to load

    Returns:
        bytes: _description_
    """
    data = load_relative_raw_file(caller_file, file_path)
    return data.decode("utf-8")


def load_relative_json_file(caller_file: str, file_path: str) -> dict[Any, Any]:
    """

    Args:
        caller_file (str): Should be called with __file__ method
        filename (str): The file path to load

    Returns:
        dict[Any, Any]: _description_
    """
    data = load_relative_raw_file(caller_file, file_path)
    return json.loads(data.decode("utf-8"))
