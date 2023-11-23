"""Module to be imported for rules to interacto with the metadata"""

from typing import Union, cast
from pydantic import TypeAdapter

from selector.model.preview_metadata import PreviewMetadata
from selector.model.preview_metadata_63 import PreviewMetadataV063, Frame

PreviewMetadataDescriminator = TypeAdapter(PreviewMetadataV063)


def parse(raw: Union[dict, str]) -> PreviewMetadata:
    """Parses a json to one of instance of preview metadata"""
    if isinstance(raw, dict):
        return cast(PreviewMetadata, PreviewMetadataDescriminator.validate_python(raw))
    return cast(PreviewMetadata, PreviewMetadataDescriminator.validate_json(raw))