"""Module to be imported for rules to interacto with the metadata"""

from typing import Union
from pydantic import parse_obj_as, parse_raw_as  # pylint: disable=no-name-in-module

from selector.model.preview_metadata import PreviewMetadata, FrameSignal
from selector.model.preview_metadata_63 import PreviewMetadataV063


def parse(raw: Union[dict, str]) -> PreviewMetadata:
    """Parses a json to one of instance of preview metadata"""
    if isinstance(raw, dict):
        return parse_obj_as(PreviewMetadataV063, raw)
    return parse_raw_as(PreviewMetadataV063, raw)
