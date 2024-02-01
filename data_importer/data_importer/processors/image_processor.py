"""Image Processor Module"""
from typing import Any
from fiftyone.core.metadata import ImageMetadata
from fiftyone import Dataset

from data_importer.processor import Processor
from data_importer.processor_repository import ProcessorRepository
from data_importer.sqs_message import SQSMessage
from data_importer.fiftyone_importer import FiftyoneImporter


# pylint: disable=too-few-public-methods
@ProcessorRepository.register(["jpg", "png", "jpeg"])
class ImageMetadataLoader(Processor):
    """
    Processor for image messages
    """

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
    def _load_metadata(cls, message: SQSMessage, **_kwargs) -> dict[str, Any]:
        return {"filepath": message.full_path, "metadata": ImageMetadata.build_for(message.full_path)}

    @classmethod
    def _upsert_sample(cls, dataset: Dataset, message: SQSMessage, metadata: dict[str, Any],
                       importer: FiftyoneImporter) -> Any:
        importer.upsert_sample(message.tenant_id, dataset, message.full_path, metadata)

    @classmethod
    def _create_dataset(cls, message: SQSMessage, importer: FiftyoneImporter, **_kwargs) -> Any:
        return importer.load_dataset(f"{message.tenant_id}-{message.dataset}", [message.tenant_id])
