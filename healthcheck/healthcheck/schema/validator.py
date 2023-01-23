# pylint: disable=too-few-public-methods
"""JSONSchema validation module."""
import json
import logging
import os
from enum import Enum
from typing import Protocol

from jsonschema import Draft202012Validator, ValidationError
from kink import inject

_logger: logging.Logger = logging.getLogger(__name__)

CURRENT_LOCATION = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))
JSONSCHEMAS_DIR = os.path.join(CURRENT_LOCATION, "jsonschema")


class Schema(Enum):
    """Available schemas."""
    ALGORITHM_OUTPUT = "algorithm-output"
    PIPELINE_EXECUTION = "pipeline-execution"
    RECORDINGS = "recordings"
    RECORDINGS_SNAPSHOT = "recordings-snapshot"
    SIGNALS = "signals"


class DocumentValidator(Protocol):
    """Document validation interface."""
    @staticmethod
    def validate_document(document: dict, schema: Schema):
        """Validates a document based on a schema"""


@inject(alias=DocumentValidator)
class JSONSchemaValidator():
    """JSONSchema validator implementation."""
    @staticmethod
    def validate_document(document: dict, schema: Schema):
        """validate_document.

        Args:
            document (dict): document object
            schema (dict): schema object

        Raises:
            val_error (ValidationError): value error
        """

        schema_path = os.path.join(JSONSCHEMAS_DIR, f"{schema.value}.json")
        with open(schema_path, "r", encoding="utf-8") as file_handler:
            jsonschema = json.load(file_handler)

        try:
            validator = Draft202012Validator(jsonschema)
            validator.validate(instance=document)
            _logger.info("valid document for schema %s", schema.name)
        except ValidationError as err:
            _logger.error("error validating document %s", err)
            raise err
