"""Default Processor module"""
from typing import Any, Optional

from base.aws.container_services import ContainerServices
from data_importer.processor import Processor
from data_importer.sqs_message import SQSMessage

_logger = ContainerServices.configure_logging(__name__)


# pylint: disable=too-few-public-methods, useless-return
class DefaultProcessor(Processor):
    """A Processor always returning None and logging a warning."""

    @classmethod
    def load_metadata(cls, message: SQSMessage, **_kwargs) -> Optional[dict[str, Any]]:
        _logger.warning("S3 file with file extension %s has no processor. Object: %s", message.file_extension, message)
        return None
