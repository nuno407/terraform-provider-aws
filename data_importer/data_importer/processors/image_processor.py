"""Image Processor Module"""
from typing import Any, Optional
from fiftyone.core.metadata import ImageMetadata

from data_importer.processor import Processor
from data_importer.processor_repository import ProcessorRepository
from data_importer.sqs_message import SQSMessage


# pylint: disable=too-few-public-methods
@ProcessorRepository.register(["jpg", "png", "jpeg"])
class ImageMetadataLoader(Processor):
    """
    Processor for image messages
    """

    @classmethod
    def load_metadata(cls, message: SQSMessage, **_kwargs) -> Optional[dict[str, Any]]:
        return {"filepath": message.full_path, "metadata": ImageMetadata.build_for(message.full_path)}
