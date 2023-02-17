from typing import Any

from data_importer.processor import Processor
from data_importer.processor_repository import ProcessorRepository
from data_importer.sqs_message import SQSMessage
from fiftyone.core.metadata import ImageMetadata


@ProcessorRepository.register(["jpg", "png", "jpeg"])
class ImageMetadataLoader(Processor):

    @classmethod
    def load_metadata(cls, message: SQSMessage, **_kwargs) -> dict[str, Any]:
        return {"filepath": message.full_path, "metadata": ImageMetadata.build_for(message.full_path)}
