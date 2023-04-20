"""Metadata Processor module"""
import json
import tempfile
import zipfile
from os import listdir, path
from os.path import dirname, isdir, isfile, join
from pathlib import Path
from typing import Any

from botocore.exceptions import ClientError

from base.aws.container_services import ContainerServices
from data_importer.fiftyone_importer import FiftyoneImporter
from data_importer.processor import Processor
from data_importer.processor_repository import ProcessorRepository
from data_importer.sqs_message import SQSMessage

_logger = ContainerServices.configure_logging(__name__)


class DatasetAlreadyExists(Exception):
    """ Exception that should be raised when already exists a dataset with the provided name. """


# pylint: disable=too-few-public-methods
@ProcessorRepository.register(["zip"])
class ZipDatasetProcessor(Processor):
    """Processor for ZIP files. This zip files should be generator via voxel export.
    Example command:
        * fiftyone datasets export DATASET_NAME -d DESTINY_FOLDER
            -t fiftyone.types.FiftyOneDataset
            -k export_media=False rel_dir="/mnt/ims/ICT_cooperation"
    """

    @classmethod
    def process(cls, message: SQSMessage, **kwargs):
        cls._process(message, **kwargs)

    @classmethod
    def _process(  # pylint: disable=too-many-locals,consider-using-with
            cls,
            message: SQSMessage,
            fiftyone_importer: FiftyoneImporter,
            s3_client: Any,
            container_services: Any,
            **_kwargs) -> Any:
        _logger.debug("full path: %s", message.full_path)
        img_dir = ""
        dataset_name = ""
        try:
            # DOWNLOAD AND EXTRACT ZIP FILE TO DISK
            temp_dir = tempfile.TemporaryDirectory()
            zip_filename = f"{temp_dir.name}/zip_file.zip"
            container_services.download_file_to_disk(  # type: ignore
                s3_client, message.bucket_name, message.file_path, zip_filename)

            with zipfile.ZipFile(zip_filename, "r") as zip_ref:
                zip_ref.extractall(temp_dir.name)

            # GET SUBFOLDER NAME (temp_dir with either dataset name or "export")
            subfolder = path.dirname(sorted(Path(temp_dir.name).glob("**/metadata.json"))[0])

            # GET NAME FROM JSON
            with open(f"{subfolder}/metadata.json", "r", encoding="utf8") as metadata_file:
                raw_metadata = json.load(metadata_file)
                dataset_name = raw_metadata["name"]
            img_dir = f"{dirname(message.file_path)}/{dataset_name}/"

            # CHECK IF DATASET ALREADY EXISTS
            full_dataset_name = f"{message.data_owner}-{dataset_name}"
            if fiftyone_importer.check_if_dataset_exists(full_dataset_name):
                raise DatasetAlreadyExists(f"Dataset {full_dataset_name} already exists")

            # SAVE IMAGES TO S3
            if isdir(f"{subfolder}/data"):  # ignore if no data is provided
                data_files = [f for f in listdir(f"{subfolder}/data") if isfile(join(subfolder, "data", f))]
                for data_file in data_files:
                    with open(f"{subfolder}/data/{data_file}", "rb") as payload:
                        container_services.upload_file(
                            s3_client, payload, message.bucket_name, f"{img_dir}data/{data_file}")
            else:
                container_services.upload_file(s3_client, "", message.bucket_name,
                                               f"{img_dir}")  # Create folder to upload data

            # LOAD DATASET FROM DIR
            dataset = fiftyone_importer.from_dir(dataset_dir=subfolder,
                                                 tags=[message.data_owner],
                                                 name=full_dataset_name,
                                                 rel_dir=f"s3://{message.bucket_name}/{img_dir}")

            # SAVE DATASET
            dataset.persistent = True
            dataset.save()
            _logger.info("Dataset metadata %s imported", full_dataset_name)

        except ClientError as ex:
            if ex.response["Error"]["Code"] == "NoSuchKey":
                _logger.warning("File cannot be found on S3 - returning None")
                return None
            raise ex
        finally:
            temp_dir.cleanup()
        return dataset
