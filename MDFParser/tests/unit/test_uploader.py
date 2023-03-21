""" Tests the Uploader class """
import json
from datetime import timedelta
from unittest.mock import ANY, Mock

from typing import Union
from pytest import fixture, mark, raises
from pytest_mock import MockerFixture
from mdfparser.uploader import Uploader

data: dict[timedelta, dict[str, Union[bool, int, float]]] = {timedelta(minutes=5): {"foo": 2}}
S3_PATH = "s3://bucket/key_metadata_full.json"


@mark.unit
class TestUploader:
    """ Tests the Uploader class """
    @fixture
    def container_services_mock(self, mocker: MockerFixture) -> Mock:
        """ Mocks the ContainerServices class """
        return mocker.patch("mdfparser.uploader.ContainerServices", autospec=True)

    @fixture
    def uploader(self) -> Uploader:
        """ Returns an instance of the Uploader class """
        return Uploader()

    def test_upload_signals(self, uploader: Uploader, container_services_mock: Mock):
        """ Tests the upload_signals method """
        # WHEN
        result = uploader.upload_signals(data, S3_PATH)

        # THEN
        expected_data = {"0:05:00": {"foo": 2}}
        expected_data_binary = json.dumps(expected_data).encode("utf-8")
        container_services_mock.upload_file.assert_called_once_with(ANY,
                                                                    expected_data_binary,
                                                                    "bucket", "key_signals.json")
        assert result == {"bucket": "bucket", "key": "key_signals.json"}

    def test_upload_invalid_path(self, uploader: Uploader, container_services_mock: Mock):
        """ Tests the upload_signals method with an invalid path """
        # GIVEN
        invalid_path = "foo_path"

        # WHEN
        with raises(ValueError) as err:
            uploader.upload_signals(data, invalid_path)

        # THEN
            assert "Invalid path" in str(err.value)
        assert container_services_mock.upload_file.call_count == 0
