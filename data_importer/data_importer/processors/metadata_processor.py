import json
from typing import Any, Optional

from base.aws.container_services import ContainerServices
from botocore.exceptions import ClientError
from data_importer.processor import Processor
from data_importer.processor_repository import ProcessorRepository
from data_importer.sqs_message import SQSMessage

_logger = ContainerServices.configure_logging(__name__)


@ProcessorRepository.register(["json"])
class JsonMetadataLoader(Processor):

    @classmethod
    def load_metadata(cls, message: SQSMessage, **kwargs) -> Optional[dict[str, Any]]:
        _logger.debug("full path: %s", message.full_path)
        try:
            raw_file = kwargs.get("container_services").download_file(
                kwargs.get("s3_client"), message.bucket_name, message.file_path)
            metadata = json.loads(raw_file)
            # We don't allow to update the filepath of the sample so metadata and media name don't get out of sync
            if "filepath" in metadata:
                metadata.pop("filepath")
            return metadata
        except ClientError as ex:
            if ex.response['Error']['Code'] == 'NoSuchKey':
                _logger.warn('File cannot be found on S3 - returning None')
                return None
            else:
                raise ex
        except json.decoder.JSONDecodeError:
            _logger.warn("Issue decoding metadata file %s", message.full_path)
        return None
