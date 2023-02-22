"""Test Processors"""
import json
import sys
from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError
from data_importer.processor_repository import ProcessorRepository
from data_importer.processors.default_processor import DefaultProcessor
from data_importer.processors.image_processor import ImageMetadataLoader
from data_importer.processors.metadata_processor import JsonMetadataLoader
from data_importer.sqs_message import SQSMessage


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

    def test_get_processor_for_unknown_extension(self):
        bumlux_processor = ProcessorRepository.get_processor("bumlux")

        isinstance(bumlux_processor, DefaultProcessor)

    def test_load_processors(self):
        ProcessorRepository.load_all_processors()


@pytest.mark.unit
class TestDefaultProcessor:
    def test_load_metadata(self):
        # GIVEN
        message = SQSMessage("principal", "test-bucket", "tmp/test/file.bumlux", "bumlux", "tmp")

        # WHEN
        metadata = DefaultProcessor.load_metadata(message)

        # THEN
        assert metadata is None


@pytest.mark.unit
class TestImageProcessor:
    def test_load_metadata(self):
        # GIVEN
        message = SQSMessage("principal", "test-bucket", "tmp/test/file.jpg", "jpg", "tmp")
        fo_metadata = sys.modules["fiftyone.core.metadata"]
        fo_metadata.ImageMetadata.build_for = Mock(return_value={"width": 111, "height": 222})

        # WHEN
        metadata = ImageMetadataLoader.load_metadata(message)

        # THEN
        assert metadata == {"filepath": "s3://test-bucket/tmp/test/file.jpg", "metadata": {"width": 111, "height": 222}}


@pytest.mark.unit
class TestMetadataProcessor:

    @pytest.fixture
    def message(self):
        return SQSMessage("principal", "test-bucket", "tmp/test/file.json", "json", "tmp")

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
        metadata = JsonMetadataLoader.load_metadata(message, container_services=container_services, s3_client=s3_client)

        # THEN
        assert metadata == {"foo": "bar", "baar": "baz"}

    def test_load_metadata_removing_filepath(self, message: SQSMessage, container_services, s3_client):
        # GIVEN
        raw_metadata = json.dumps({"foo": "bar", "filepath": "path/to/new.jpg"})
        container_services.download_file = Mock(return_value=raw_metadata)

        # WHEN
        metadata = JsonMetadataLoader.load_metadata(message, container_services=container_services, s3_client=s3_client)

        # THEN
        assert metadata == {"foo": "bar"}

    def test_load_metadata_missing_s3_file(self, message: SQSMessage, container_services, s3_client):
        # GIVEN
        container_services.download_file = Mock(side_effect=ClientError({"Error": {"Code": "NoSuchKey"}}, "get_object"))

        # WHEN
        metadata = JsonMetadataLoader.load_metadata(message, container_services=container_services, s3_client=s3_client)

        # THEN
        assert metadata is None

    def test_load_metadata_fails_on_unknown_error(self, message: SQSMessage, container_services, s3_client):
        # GIVEN
        container_services.download_file = Mock(
            side_effect=ClientError({"Error": {"Code": "500", "Message": "foo"}}, "get_object"))

        # WHEN
        with pytest.raises(ClientError):
            JsonMetadataLoader.load_metadata(message, container_services=container_services, s3_client=s3_client)

    def test_load_metadata_on_invalid_raw_data(self, message: SQSMessage, container_services, s3_client):
        # GIVEN
        raw_metadata = "{\"foo\": \"bar\", broken"
        container_services.download_file = Mock(return_value=raw_metadata)

        # WHEN
        metadata = JsonMetadataLoader.load_metadata(message, container_services=container_services, s3_client=s3_client)

        # THEN
        assert metadata is None
