from typing import Any


def flatten_dict(data: dict[Any,Any]) -> dict[Any,Any]:
    """
    Flattens a nested dictionary into a single level dictionary.
    List and dictionary are seperated by "." and list index is used as key.

    Args:
        data (dict[Any,Any]): Data to flatten

    Returns:
        dict[Any,Any]: Flattened dictionary
    """
    mem: dict[Any,Any] = {}

    for key,val in data.items():
        __flatten_dict_recursive(val, key, mem)
    return mem


def __flatten_dict_recursive(data: Any, flatten_key: str, mem: dict[Any,Any]) -> None:
    """
    Uses a recursive approach to merge dicitonary and lists.

    Args:
        data (Any): data to flatten
        flatten_key (str): key keep track trough out the itreation
        mem (dict[Any,Any]): memory to store the flatten data
    """
    if isinstance(data, dict):
        if len(data) == 0:
            mem[flatten_key] = {}
        for key,val in data.items():
            tmp_flatten_key = f"{flatten_key}.{key}"
            __flatten_dict_recursive(val, tmp_flatten_key, mem)

    elif isinstance(data, list):
        if len(data) == 0:
            mem[flatten_key] = []
        for i, item in enumerate(data):
            tmp_flatten_key = f"{flatten_key}.{i}"
            __flatten_dict_recursive(item, tmp_flatten_key, mem)
    else:
        mem[flatten_key] = data
