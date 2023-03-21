""" Test mdfparser """
import json
import os
from unittest.mock import ANY, Mock, PropertyMock

from pytest import LogCaptureFixture, fixture, mark
from pytest_mock import MockerFixture
from mdfparser.main import main
from mdfparser.config import MdfParserConfig

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

RECORDING_FROM = 1659962815000
RECORDING_TO = 1659962819000
RECORDING_NAME = "tenant_recording_" + str(RECORDING_FROM) + "_" + str(RECORDING_TO)
S3_PATH = "s3://bucket/" + RECORDING_NAME + "_metadata_full.json"


@mark.integration
class TestMain:
    """ Test main """
    @fixture
    def boto3_mock(self, mocker: MockerFixture):
        """ Mock boto3 for all tests in this class """
        _ = mocker.patch("mdfparser.main.boto3")
        return mocker.patch("mdfparser.s3_interaction.boto3")

    @fixture
    def container_services_mock(self, mocker: MockerFixture, boto3_mock: Mock) -> Mock:  # pylint: disable=unused-argument
        """ Mock ContainerServices for all tests in this class """
        container_services_mock = mocker.patch("mdfparser.main.ContainerServices", autospec=True)
        mocker.patch("mdfparser.downloader.ContainerServices", container_services_mock)
        mocker.patch("mdfparser.uploader.ContainerServices", container_services_mock)
        return container_services_mock

    @fixture
    def graceful_exit_mock(self, mocker: MockerFixture) -> Mock:
        """ Mock GracefulExit for all tests in this class """
        graceful_exit_mock = mocker.patch("mdfparser.main.GracefulExit")
        continue_running_mock = PropertyMock(side_effect=[True, False])
        type(graceful_exit_mock.return_value).continue_running = continue_running_mock
        return continue_running_mock

    @fixture
    def mdf_parser_config(self) -> MdfParserConfig:
        """ Return a MdfParserConfig for all tests in this class """
        return MdfParserConfig(
            input_queue="dev-terraform-queue-mdf-parser",
            metadata_output_queue="dev-terraform-queue-metadata")

    def test_mdf_parsing(self, container_services_mock: Mock, graceful_exit_mock: PropertyMock,
                         mdf_parser_config: MdfParserConfig):
        """ Test mdf parsing """
        ### GIVEN ###
        # data preparation
        cs_mock = container_services_mock

        given_path = os.path.join(__location__, "test_data/mdf_synthetic.json")
        with open(given_path, "r", encoding="utf-8") as f_handler:
            mdf_data_encoded = f_handler.read().encode("utf-8")

        message = {
            "Body": json.dumps({
                "_id": RECORDING_NAME,
                "s3_path": S3_PATH
            }).replace("\"", "\'"),
            "ReceiptHandle": "receipt_handle"
        }

        # input queue mocks
        cs_mock.return_value.get_single_message_from_input_queue.return_value = message
        # downloader mocks
        cs_mock.download_file.return_value = mdf_data_encoded

        ### WHEN ###
        main(config=mdf_parser_config)

        ### THEN ###
        expect_update_path = os.path.join(__location__, "test_data/recording_update_expected.json")
        with open(expect_update_path, "r", encoding="utf-8") as f_handler:
            expected_update = json.loads(f_handler.read())

        expect_sync_path = os.path.join(__location__, "test_data/sync_expected.json")
        with open(expect_sync_path, "r", encoding="utf-8") as f_handler:
            expected_sync = json.dumps(json.loads(f_handler.read().encode("utf-8"))).encode("utf-8")

        cs_mock.return_value \
            .send_message.assert_called_once_with(
                ANY, "dev-terraform-queue-metadata", expected_update)
        cs_mock.return_value\
            .delete_message.assert_called_once_with(
                ANY, message["ReceiptHandle"], "dev-terraform-queue-mdf-parser")
        cs_mock.upload_file\
            .assert_called_once_with(
                ANY, expected_sync, "bucket", RECORDING_NAME + "_signals.json")
        assert graceful_exit_mock.call_count == 2

    def test_mdf_parsing_invalid_path(self, container_services_mock: Mock,
                                      graceful_exit_mock,
                                      caplog: LogCaptureFixture,
                                      mdf_parser_config: MdfParserConfig):
        """ Test MDF parsing invalid path """
        ### GIVEN ###
        message = {
            "Body": json.dumps({
                "_id": RECORDING_NAME,
                "s3_path": "i_am_invalid"
            }).replace("\"", "\'"),
            "ReceiptHandle": "receipt_handle"
        }

        # input queue mocks
        container_services_mock.return_value \
            .get_single_message_from_input_queue.return_value = message

        ### WHEN ###
        main(config=mdf_parser_config)

        ### THEN ###
        assert bool([
            "Invalid MDF path" in rec.message
            for rec in caplog.records if rec.levelname == "ERROR"
        ])
        container_services_mock.return_value.send_message.assert_not_called()
        container_services_mock.return_value.delete_message.assert_not_called()
        container_services_mock.upload_file.assert_not_called()
        assert graceful_exit_mock.call_count == 2

    def test_mdf_parsing_download_error(self, container_services_mock: Mock,
                                        graceful_exit_mock,
                                        caplog: LogCaptureFixture,
                                        mdf_parser_config: MdfParserConfig):
        """ Test MDF parsing download error """
        ### GIVEN ###
        message = {
            "Body": json.dumps({
                "_id": RECORDING_NAME,
                "s3_path": S3_PATH
            }).replace("\"", "\'"),
            "ReceiptHandle": "receipt_handle"
        }

        # input queue mocks
        container_services_mock.return_value \
            .get_single_message_from_input_queue.return_value = message
        # downloader mocks
        container_services_mock.download_file.side_effect = Exception("Download error")

        ### WHEN ###
        main(config=mdf_parser_config)

        ### THEN ###
        assert bool([
            "Download error" in rec.message
            for rec in caplog.records
            if rec.levelname == "ERROR"
        ])
        container_services_mock.return_value.send_message.assert_not_called()
        container_services_mock.return_value.delete_message.assert_not_called()
        container_services_mock.upload_file.assert_not_called()
        assert graceful_exit_mock.call_count == 2
