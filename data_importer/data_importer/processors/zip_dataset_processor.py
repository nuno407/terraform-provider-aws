"""Metadata Processor module"""
import json
import logging
import os
import tempfile
import zipfile
from os import path
from os.path import dirname, isdir
from pathlib import Path
from typing import Any

from botocore.exceptions import ClientError

from base.aws.container_services import ContainerServices
from data_importer.fiftyone_importer import FiftyoneImporter
from data_importer.processor import Processor
from data_importer.processor_repository import ProcessorRepository
from data_importer.sqs_message import SQSMessage

DOWNLOAD_STORAGE_PATH = os.getenv("DOWNLOAD_STORAGE_PATH", None)
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
        _logger.info("Zip upload detected, going to import it")
        _logger.debug("Message full path: %s", message.full_path)
        try:
            # DOWNLOAD AND EXTRACT ZIP FILE TO DISK
            temp_dir = tempfile.TemporaryDirectory(dir=DOWNLOAD_STORAGE_PATH)
            zip_filename = f"{temp_dir.name}/zip_file.zip"
            container_services.download_file_to_disk(  # type: ignore
                s3_client, message.bucket_name, message.file_path, zip_filename)
            _logger.info("Downloaded zip to %s", zip_filename)

            with zipfile.ZipFile(zip_filename, "r") as zip_ref:
                zip_ref.extractall(temp_dir.name)
            _logger.info("Unzipped successfully")

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
                log_message = f"Dataset {full_dataset_name} already exists"
                _logger.warning(log_message)
                raise DatasetAlreadyExists(log_message)

            # SAVE IMAGES TO S3
            _logger.info("Uploading footage to S3")
            footage_dir = f"{subfolder}/data/"
            if isdir(footage_dir):  # ignore if no data is provided
                for subdir, _dirs, files in os.walk(footage_dir):
                    for file in files:
                        data_file = os.path.join(subdir, file)
                        with open(data_file, "rb") as payload:
                            relative_footage_path = ZipDatasetProcessor.__remove_prefix(data_file, footage_dir)
                            container_services.upload_file(s3_client, payload, message.bucket_name,
                                                           f"{img_dir}data/{relative_footage_path}",
                                                           log_level=logging.DEBUG)
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

    @classmethod
    def __remove_prefix(cls, text: str, prefix: str) -> str:
        """
        Removes the prefix of a string if it exists
        :param text: String to remove prefix from
        :param prefix: Prefix to remove
        :return: String without prefix or initial string if prefix was not found
        """
        if text.startswith(prefix):
            return text[len(prefix):]
        return text
