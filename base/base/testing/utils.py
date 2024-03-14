"""Utility functions to be used on tests"""
import json
import os
import io
from typing import TYPE_CHECKING, Any, Generic, TypeVar
import pandas as pd

if TYPE_CHECKING:
    from pytest import FixtureRequest as PytestRequest
    T = TypeVar("T")

    class FixtureRequest(PytestRequest, Generic[T]):
        """ Fixture Request"""
        param: T
else:
    from pytest import FixtureRequest


def get_abs_path(caller_file: str, file_path: str) -> str:
    """

    Args:
        caller_file (str): Should be called with __file__ method
        filename (str): The file path to load

    Returns:
        str: THe absolute path to the file
    """
    return os.path.join(os.path.dirname(os.path.abspath(caller_file)), file_path)


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


def load_relative_str_file(caller_file: str, file_path: str) -> str:
    """

    Args:
        caller_file (str): Should be called with __file__ method
        filename (str): The file path to load

    Returns:
        str: _description_
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


def assert_unordered_lists(list_a: list[Any], list_b: list[Any]) -> None:
    """
    Asserts that two lists are equal, regardless of their order.
    Also supports unhashable types.

    Args:
        list_a (list[Any]): _description_
        list_b (list[Any]): _description_
    """
    for item in list_a:
        assert item in list_b, f"Item {item} not in \n {list_b}"
    assert len(list_a) == len(list_b)


def assert_parquet_streams(stream_a: str | io.BytesIO, stream_b: str | io.BytesIO, atol: float = 0.00001) -> None:
    """
    Asserts that two parquet streams are equal

    Args:
        stream_a: _description_
        stream_b: _description_
    """
    df_a = pd.read_parquet(stream_a, engine="fastparquet")
    df_b = pd.read_parquet(stream_b, engine="fastparquet")
    pd.testing.assert_frame_equal(df_a, df_b, check_exact=False, atol=atol)
