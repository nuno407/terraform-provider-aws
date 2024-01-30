"""Metadata Handler class"""
import logging
import re
from typing import Any, Union

from kink import inject

from mergedeep import merge as recursive_merge

from base.aws.container_services import ContainerServices
from base.processor import Processor
from mdfparser.exceptions import (InvalidFileNameException,
                                  NoProcessingSuccessfulException)
from mdfparser.interfaces.handler import Handler
from mdfparser.interfaces.input_message import DataType, InputMessage
from mdfparser.interfaces.output_message import OutputMessage
from mdfparser.metadata.downloader import MetadataDownloader
from mdfparser.metadata.synchronizer import Synchronizer
from mdfparser.metadata.uploader import MetadataUploader

_logger: logging.Logger = ContainerServices.configure_logging(__file__)


@inject
class MetadataHandler(Handler):
    """
    The Handler used to process the Metadata full
    """

    def __init__(
            self,
            metadata_downloader: MetadataDownloader,
            metadata_uploader: MetadataUploader,
            syncronizer: Synchronizer,
            processor_list: list[Processor]):
        self.downloader = metadata_downloader
        self.uploader = metadata_uploader
        self.synchronizer = syncronizer
        self.processors: list[Processor] = processor_list

    def ingest(self, message: InputMessage) -> OutputMessage:
        """
        Ingest and parses the metadata.

        Args:
            message (InputMessage): The message from the SQS queue.

        Returns:
            OutputMessage: The message to be sent to the Metadata service.
        """
        metadata_file, processors = self.process_request(message.s3_path)
        return OutputMessage(
            message.id,
            metadata_file,
            self.handler_type(),
            processors,
            message.tenant,
            message.raw_s3_path)

    def handler_type(self) -> DataType:
        """
        The Handler type used to match the Metadata messages.

        Returns:
            DataType: The Datatype to be processed by this handler.
        """
        return DataType.METADATA

    def process_request(self, mdf_s3_path: str) -> tuple[str, dict[str, Union[float, int]]]:
        """
        Process the metadata full file

        Args:
            mdf_s3_path (str): The path to the metadata_full file.

        Raises:
            NoProcessingSuccessfulException: Error while processing the the metadata file.

        Returns:
            tuple[
                str,
                dict[
                    str,
                    Union[float, int]
                ]
            ]: A tuple containg the file with the signals uploaded and a dictionary containg the processing made.
        """
        _logger.info("Starting processing of metadata for %s", mdf_s3_path)

        # download and synchronize metadata
        mdf = self.downloader.download(mdf_s3_path)
        timestamp_from, timestamp_to = self.extract_timestamps(mdf_s3_path)
        synchronized = self.synchronizer.synchronize(mdf, timestamp_from, timestamp_to)

        # compute updated metadata
        metadata: dict[str, Any] = {}

        successful_processings: int = 0
        for processor in self.processors:
            try:
                process_output = processor.process(synchronized)
                recursive_merge(metadata, process_output)
                successful_processings += 1
            except Exception:  # pylint: disable=broad-except
                # we do not want the entire recording to fail for a specific processing only
                _logger.exception("Error processing metadata.")
        if successful_processings == 0:
            raise NoProcessingSuccessfulException(
                "Not a single processing succeeded, therefore not updating metadata.")  # pylint: disable=line-too-long

        _logger.info("Successfully processed metadata for %s", mdf_s3_path)

        if "recording_overview" in metadata:
            _logger.debug("Recording overview fields: %s", str(metadata["recording_overview"]))

        # upload synchronized signals to s3 and store path in metadata
        try:
            signals_path = self.uploader.upload_signals(synchronized, mdf_s3_path)

            return signals_path, metadata.get("recording_overview", {})
        except Exception as excpt:
            _logger.exception("Error uploading synchronized signals to S3.")
            raise excpt

    @staticmethod
    def extract_timestamps(filepath: str) -> tuple[int, int]:
        """Extracts the timestamps from the filepath."""
        match = re.search(r"_(\d{13,})_(\d{13,})_", filepath)
        if not match or len(match.groups()) < 2:
            raise InvalidFileNameException("Cannot extract timestamps from filepath \"" +
                                           filepath + "\".")  # pylint: disable=line-too-long
        timestamp_from = int(match.group(1))
        timestamp_to = int(match.group(2))
        return timestamp_from, timestamp_to
