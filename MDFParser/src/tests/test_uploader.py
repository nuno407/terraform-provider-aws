from datetime import timedelta
import json
from unittest.mock import ANY, Mock
from pytest import fixture, raises

from pytest_mock import MockerFixture
from mdfparser.uploader import Uploader

data = { timedelta(minutes=5): {'foo': 2} }
s3_path = 's3://bucket/key_metadata_full.json'

class TestUploader:
    @fixture
    def container_services_mock(self, mocker: MockerFixture) -> Mock:
        return mocker.patch('mdfparser.uploader.ContainerServices', autospec=True)

    @fixture
    def uploader(self) -> Uploader:
        return Uploader()

    def test_upload_signals(self, uploader: Uploader, container_services_mock: Mock):
        # WHEN
        result = uploader.upload_signals(data, s3_path)

        # THEN
        expected_data = { "0:05:00": {'foo': 2} }
        expected_data_binary = json.dumps(expected_data).encode('utf-8')
        container_services_mock.upload_file.assert_called_once_with(ANY, expected_data_binary, 'bucket', 'key_signals.json')
        assert(result == {'bucket': 'bucket', 'key': 'key_signals.json'})

    def test_upload_invalid_path(self, uploader: Uploader, container_services_mock: Mock):
        # GIVEN
        invalid_path = 'foo_path'

        # WHEN
        with raises(ValueError) as e:
            uploader.upload_signals(data, invalid_path)

        # THEN
            assert("Invalid path") in str(e.value)
        assert(container_services_mock.upload_file.call_count == 0)
