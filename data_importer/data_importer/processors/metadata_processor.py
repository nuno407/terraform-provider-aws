"""Metadata Processor module"""
import json
from typing import Any, Optional
from botocore.exceptions import ClientError
from fiftyone import Dataset

from base.aws.container_services import ContainerServices

from data_importer.fiftyone_importer import FiftyoneImporter
from data_importer.processor import Processor
from data_importer.processor_repository import ProcessorRepository
from data_importer.sqs_message import SQSMessage

_logger = ContainerServices.configure_logging(__name__)


# pylint: disable=too-few-public-methods,duplicate-code
@ProcessorRepository.register(["json"])
class JsonMetadataLoader(Processor):
    """Processor for JSON metadata files. Gets the metadata from S3 and converts it to a dictionary."""

    # These fields should not be changed by the user provided metadata
    sanitized_fields = ["filepath", "id", "media_type", "metadata"]

    @classmethod
    def process(cls, message: SQSMessage, **kwargs):
        cls._process(message, **kwargs)

    @classmethod
    def _process(cls, message: SQSMessage, fiftyone_importer: FiftyoneImporter, **kwargs):
        metadata = cls._load_metadata(message, **kwargs)

        dataset = cls._create_dataset(message, fiftyone_importer, **kwargs)

        # Find or create a new Sample with the given metadata
        cls._upsert_sample(dataset, message, metadata, fiftyone_importer)

    @classmethod
    def _load_metadata(cls, message: SQSMessage, container_services: ContainerServices,
                       s3_client, **_kwargs) -> Optional[dict[str, Any]]:
        _logger.debug("full path: %s", message.full_path)
        try:
            raw_file = container_services.download_file(  # type: ignore
                s3_client, message.bucket_name, message.file_path)
            metadata = json.loads(raw_file)
            ignored_fields = list(filter(lambda field: field in cls.sanitized_fields, metadata))
            for field in ignored_fields:
                # Ignoring important Sample internal fields
                metadata.pop(field)
            return metadata
        except ClientError as ex:
            if ex.response["Error"]["Code"] == "NoSuchKey":
                _logger.warning("File cannot be found on S3 - returning None")
                return None
            raise ex
        except json.decoder.JSONDecodeError:
            _logger.warning("Issue decoding metadata file %s", message.full_path)
        return None

    @classmethod
    def _upsert_sample(cls, dataset: Dataset, message: SQSMessage, metadata: Optional[dict[str, Any]],
                       importer: FiftyoneImporter) -> Any:
        importer.replace_sample(message.tenant_id, dataset, message.full_path, metadata)

    @classmethod
    def _create_dataset(cls, message: SQSMessage, importer: FiftyoneImporter, **_kwargs) -> Any:
        return importer.load_dataset(f"{message.tenant_id}-{message.dataset}", [message.tenant_id])
