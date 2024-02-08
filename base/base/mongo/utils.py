from typing import Any


def flatten_dict(data: dict[Any,Any]) -> dict[Any,Any]:
    """
    Flattens a nested dictionary into a single level dictionary.
    Uses a recursive approach and
    """
    result = {}
    for key in data:
        if isinstance(data[key], dict):
            flattened = flatten_dict(data[key])
            for sub_key in flattened:
                result[f"{key}.{sub_key}"] = flattened[sub_key]
        else:
            result[key] = data[key]
    return result
