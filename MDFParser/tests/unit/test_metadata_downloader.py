""" Unit tests for the Downloader class. """
import json
from unittest.mock import ANY, Mock
from pytest import LogCaptureFixture, fixture, raises, mark
from mdfparser.metadata.downloader import MetadataDownloader

S3_PATH = "s3://bucket/key_metadata_full.json"


@mark.usefixtures("metadata_downloader", "s3_client", "container_services_mock")
@mark.unit
class TestMetadataDownloader():
    """ Tests the Downloader class. """

    def test_download(
            self,
            metadata_downloader: MetadataDownloader,
            container_services_mock: Mock):
        """ Tests the download method. """
        # GIVEN
        data = {
            "foo": "bar",
            "test": "hello",
            "chunk": {
                "pts_start": 1,
                "pts_end": 2,
                "utc_start": 3,
                "utc_end": 4
            }
        }
        data_binary = json.dumps(data).encode("utf-8")

        container_services_mock.download_file.return_value = data_binary

        # WHEN
        result = metadata_downloader.download(S3_PATH)

        # THEN
        container_services_mock.download_file.assert_called_once_with(ANY, "bucket",
                                                                      "key_metadata_full.json")
        assert result == data

    def test_download_with_recreation_of_timestamps(self, metadata_downloader: MetadataDownloader,
                                                    container_services_mock: Mock):
        """ Tests the download method with the recreation of timestamps. """
        # GIVEN
        data = {"foo": "bar", "test": "hello", "chunk": {"pts_start": 1, "pts_end": 4}}
        data_binary = json.dumps(data).encode("utf-8")
        compact_data = {
            "partial_timestamps": {
                "1": {
                    "pts_start": 2,
                    "converted_time": 200
                },
                "2": {"pts_start": 3,
                      "converted_time": 300
                      }
            }
        }
        compact_data_binary = json.dumps(compact_data).encode("utf-8")

        container_services_mock.download_file.side_effect = [data_binary, compact_data_binary]

        # WHEN
        result = metadata_downloader.download(S3_PATH)

        # THEN
        assert container_services_mock.download_file.call_count == 2
        first_call = container_services_mock.download_file.call_args_list[0]
        assert first_call.args[1] == "bucket"
        assert first_call.args[2] == "key_metadata_full.json"
        second_call = container_services_mock.download_file.call_args_list[1]
        assert second_call.args[1] == "bucket"
        assert second_call.args[2] == "key_compact_mdf.json"
        assert result["chunk"]["utc_start"] == 100
        assert result["chunk"]["utc_end"] == 400

    def test_invalid_compact_mdf(self, caplog: LogCaptureFixture, metadata_downloader: MetadataDownloader,
                                 container_services_mock: Mock):
        """ Tests the download method with an invalid compact MDF. """
        # GIVEN
        data = {"foo": "bar", "test": "hello", "chunk": {"pts_start": 1, "pts_end": 4}}
        data_binary = json.dumps(data).encode("utf-8")
        compact_data = {"foo": "bar"}
        compact_data_binary = json.dumps(compact_data).encode("utf-8")

        container_services_mock.download_file.side_effect = [data_binary, compact_data_binary]

        # WHEN
        metadata_downloader.download(S3_PATH)
        assert "Error recreating epoch timestamps from compact MDF" in caplog.text

    def test_invalid_path(self, metadata_downloader: MetadataDownloader, container_services_mock: Mock):
        """ Tests the download method with an invalid path. """
        # GIVEN
        invalid_path = "foo_path"

        # WHEN
        with raises(ValueError) as err:
            metadata_downloader.download(invalid_path)

        # THEN
            assert "Invalid path" in str(err.value)
        container_services_mock.download_file.assert_not_called()
