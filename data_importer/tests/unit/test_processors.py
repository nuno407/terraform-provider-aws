"""Test Processors"""
import json
import logging
import os
import shutil
import sys
from unittest.mock import ANY, MagicMock, Mock

import pytest
from botocore.exceptions import ClientError

from data_importer.fiftyone_importer import FiftyoneImporter
from data_importer.processor_repository import ProcessorRepository
from data_importer.processors.default_processor import DefaultProcessor
from data_importer.processors.image_processor import ImageMetadataLoader
from data_importer.processors.metadata_processor import JsonMetadataLoader
from data_importer.processors.zip_dataset_processor import ZipDatasetProcessor
from data_importer.sqs_message import SQSMessage

CURRENT_LOCATION = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))
DATA = os.path.join(CURRENT_LOCATION, "data")

# pylint: disable=missing-function-docstring, missing-class-docstring, too-few-public-methods


@pytest.mark.unit
class TestProcessorRepositoryLoading:

    @pytest.fixture(scope="session", autouse=True)
    def processors(self):
        ProcessorRepository.load_all_processors()

    def test_get_processor_for_json_extension(self):
        json_processor = ProcessorRepository.get_processor("json")

        isinstance(json_processor, JsonMetadataLoader)

    def test_get_processor_for_jpg_extension(self):
        jpg_processor = ProcessorRepository.get_processor("jpg")

        isinstance(jpg_processor, ImageMetadataLoader)

    def test_get_processor_for_zip_extension(self):
        zip_processor = ProcessorRepository.get_processor("zip")

        isinstance(zip_processor, ZipDatasetProcessor)

    def test_get_processor_for_unknown_extension(self):
        bumlux_processor = ProcessorRepository.get_processor("bumlux")

        isinstance(bumlux_processor, DefaultProcessor)

    def test_load_processors(self):
        ProcessorRepository.load_all_processors()


@pytest.mark.unit
class TestDefaultProcessor:
    def test_process(self):
        # GIVEN
        message = SQSMessage("principal", "dev-tenant_id-raw", "tmp/test/file.bumlux", "bumlux", "tmp", "TENANT_ID")

        # WHEN
        result = DefaultProcessor.process(message)

        # THEN
        assert result is None


@pytest.mark.unit
class TestImageProcessor:
    def test_load_metadata(self):
        # GIVEN
        message = SQSMessage("principal", "dev-tenant_id-raw", "tmp/test/file.jpg", "jpg", "tmp", "TENANT_ID")
        fo_metadata = sys.modules["fiftyone.core.metadata"]
        fo_metadata.ImageMetadata.build_for = Mock(return_value={"width": 111, "height": 222})

        # WHEN
        metadata = ImageMetadataLoader._load_metadata(message)  # type: ignore # pylint: disable=protected-access

        # THEN
        assert metadata == {
            "filepath": "s3://dev-tenant_id-raw/tmp/test/file.jpg",
            "metadata": {
                "width": 111,
                "height": 222}}


@pytest.mark.unit
class TestMetadataProcessor:

    @pytest.fixture
    def message(self):
        return SQSMessage("principal", "dev-tenant_id-raw", "tmp/test/file.json", "json", "tmp", "TENANT_ID")

    @pytest.fixture
    def container_services(self):
        return Mock()

    @pytest.fixture
    def s3_client(self):
        return Mock()

    def test_load_metadata(self, message: SQSMessage, container_services, s3_client):
        # GIVEN
        raw_metadata = json.dumps({"foo": "bar", "baar": "baz"})
        container_services.download_file = Mock(return_value=raw_metadata)

        # WHEN
        metadata = JsonMetadataLoader._load_metadata(  # type: ignore # pylint: disable=protected-access
            message, container_services=container_services, s3_client=s3_client)

        # THEN
        assert metadata == {"foo": "bar", "baar": "baz"}

    def test_load_metadata_removing_prohibited_fields(self, message: SQSMessage, container_services, s3_client):
        # GIVEN
        raw_metadata = json.dumps({
            "foo": "bar",
            "filepath": "path/to/new.jpg",
            "id": "1234",
            "media_type": "test",
            "metadata": {
                "nested": "stuff"
            }
        })
        container_services.download_file = Mock(return_value=raw_metadata)

        # WHEN
        metadata = JsonMetadataLoader._load_metadata(  # type: ignore # pylint: disable=protected-access
            message, container_services=container_services, s3_client=s3_client)

        # THEN
        assert metadata == {"foo": "bar"}

    def test_load_metadata_missing_s3_file(self, message: SQSMessage, container_services, s3_client):
        # GIVEN
        container_services.download_file = Mock(side_effect=ClientError({"Error": {"Code": "NoSuchKey"}}, "get_object"))

        # WHEN
        metadata = JsonMetadataLoader._load_metadata(  # type: ignore # pylint: disable=protected-access
            message, container_services=container_services, s3_client=s3_client)

        # THEN
        assert metadata is None

    def test_load_metadata_fails_on_unknown_error(self, message: SQSMessage, container_services, s3_client):
        # GIVEN
        container_services.download_file = Mock(
            side_effect=ClientError({"Error": {"Code": "500", "Message": "foo"}}, "get_object"))

        # WHEN
        with pytest.raises(ClientError):
            JsonMetadataLoader._load_metadata(  # type: ignore # pylint: disable=protected-access
                message,
                container_services=container_services,
                s3_client=s3_client)

    def test_load_metadata_on_invalid_raw_data(self, message: SQSMessage, container_services, s3_client):
        # GIVEN
        raw_metadata = "{\"foo\": \"bar\", broken"
        container_services.download_file = Mock(return_value=raw_metadata)

        # WHEN
        metadata = JsonMetadataLoader._load_metadata(  # type: ignore # pylint: disable=protected-access
            message, container_services=container_services, s3_client=s3_client)

        # THEN
        assert metadata is None


@pytest.mark.unit
class TestZipDatasetProcessor:
    @pytest.fixture
    def message(self):
        return SQSMessage("principal", "dev-tenant_id-raw", "batches/file.zip", "zip", "tmp", "TENANT_ID")

    @pytest.fixture
    def container_services(self):
        return Mock()

    @pytest.fixture
    def s3_client(self):
        return Mock()

    @pytest.fixture
    def mock_download_file_to_disk(self):

        # pylint: disable=unused-argument
        def _mock_download_file_to_disk(s3_client, bucket_name, s3_path, file_name):
            shutil.copy(os.path.join(DATA, "test_zip.zip"), file_name)

        return _mock_download_file_to_disk

    def test_process(self, message: SQSMessage, container_services, s3_client, mock_download_file_to_disk):
        # GIVEN
        importer = FiftyoneImporter()
        container_services.download_file_to_disk = MagicMock(side_effect=mock_download_file_to_disk)
        container_services.upload_file = Mock()
        importer.check_if_dataset_exists = Mock(return_value=False)  # type: ignore
        importer.from_dir = Mock()  # type: ignore

        # WHEN
        ZipDatasetProcessor.process(
            message,
            fiftyone_importer=importer,
            s3_client=s3_client,
            container_services=container_services)

        # THEN
        container_services.download_file_to_disk.assert_called_once_with(
            s3_client, message.bucket_name, message.file_path, ANY)
        importer.check_if_dataset_exists.assert_called_once_with("TENANT_ID-RC-datanauts_snapshots")
        container_services.upload_file.assert_any_call(
            s3_client,
            ANY,
            message.bucket_name,
            "batches/RC-datanauts_snapshots/data/datanauts_DATANAUTS_DEV_01_TrainingMultiSnapshot_TrainingMultiSnapshot-49f1bbe8-7329-4a3d-8e2f-e1359951397e_1_1681220927210_anonymized.jpeg",  # pylint: disable=line-too-long
            log_level=logging.DEBUG)
        container_services.upload_file.assert_any_call(
            s3_client,
            ANY,
            message.bucket_name,
            "batches/RC-datanauts_snapshots/data/datanauts_DATANAUTS_DEV_02_TrainingMultiSnapshot_TrainingMultiSnapshot-5a8a8f3d-5f81-4efb-8c68-a504687454b0_1_1680795110813_anonymized.jpeg",  # pylint: disable=line-too-long
            log_level=logging.DEBUG)
        importer.from_dir.assert_called_once_with(
            dataset_dir=ANY,
            tags=["TENANT_ID"],
            name="TENANT_ID-RC-datanauts_snapshots",
            rel_dir=f"s3://{message.bucket_name}/batches/RC-datanauts_snapshots/")
