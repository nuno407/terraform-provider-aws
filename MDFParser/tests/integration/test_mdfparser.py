import json
import os
from unittest.mock import ANY, Mock, PropertyMock, patch

from pytest import LogCaptureFixture, fixture, mark
from pytest_mock import MockerFixture
from mdfparser.main import main
from mdfparser.config import MdfParserConfig

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

recording_from = 1659962815000
recording_to = 1659962819000
recording_name = 'tenant_recording_' + str(recording_from) + '_' + str(recording_to)
s3_path = 's3://bucket/' + recording_name + '_metadata_full.json'


@mark.integration
class TestMain:
    @fixture
    def boto3_mock(self, mocker: MockerFixture):
        mock = mocker.patch('mdfparser.main.boto3')
        return mocker.patch('mdfparser.s3_interaction.boto3')

    @fixture
    def container_services_mock(self, mocker: MockerFixture, boto3_mock: Mock) -> Mock:
        container_services_mock = mocker.patch('mdfparser.main.ContainerServices', autospec=True)
        mocker.patch('mdfparser.downloader.ContainerServices', container_services_mock)
        mocker.patch('mdfparser.uploader.ContainerServices', container_services_mock)
        return container_services_mock

    @fixture
    def graceful_exit_mock(self, mocker: MockerFixture) -> Mock:
        graceful_exit_mock = mocker.patch('mdfparser.main.GracefulExit')
        continue_running_mock = PropertyMock(side_effect=[True, False])
        type(graceful_exit_mock.return_value).continue_running = continue_running_mock
        return continue_running_mock

    @fixture
    def mdf_parser_config(self) -> MdfParserConfig:
        return MdfParserConfig(input_queue='dev-terraform-queue-mdf-parser', metadata_output_queue='dev-terraform-queue-metadata')

    def test_mdf_parsing(self, mocker: MockerFixture, container_services_mock: Mock, graceful_exit_mock: PropertyMock, mdf_parser_config: MdfParserConfig):
        ### GIVEN ###
        # data preparation
        with open(os.path.join(__location__, 'test_data/mdf_synthetic.json'), 'r') as f:
            mdf_data_encoded = f.read().encode('utf-8')

        message = {
            'Body': json.dumps({
                '_id': recording_name,
                's3_path': s3_path
            }).replace('\"', '\''),
            'ReceiptHandle': 'receipt_handle'
        }

        # input queue mocks
        container_services_mock.return_value.listen_to_input_queue.return_value = message
        # downloader mocks
        container_services_mock.download_file.return_value = mdf_data_encoded

        ### WHEN ###
        main(config=mdf_parser_config)

        ### THEN ###
        with open(os.path.join(__location__, 'test_data/recording_update_expected.json'), 'r') as f:
            expected_update = json.loads(f.read())
        with open(os.path.join(__location__, 'test_data/sync_expected.json'), 'r') as f:
            expected_sync = json.dumps(json.loads(f.read().encode('utf-8'))).encode('utf-8')

        container_services_mock.return_value.send_message.assert_called_once_with(ANY, 'dev-terraform-queue-metadata', expected_update)
        container_services_mock.return_value.delete_message.assert_called_once_with(ANY, message['ReceiptHandle'], 'dev-terraform-queue-mdf-parser')
        container_services_mock.upload_file.assert_called_once_with(ANY, expected_sync, 'bucket', recording_name + '_signals.json')
        assert(graceful_exit_mock.call_count == 2)

    def test_mdf_parsing_invalid_path(self, mocker: MockerFixture, container_services_mock: Mock, graceful_exit_mock, caplog: LogCaptureFixture, mdf_parser_config: MdfParserConfig):
        ### GIVEN ###
        message = {
            'Body': json.dumps({
                '_id': recording_name,
                's3_path': 'i_am_invalid'
            }).replace('\"', '\''),
            'ReceiptHandle': 'receipt_handle'
        }

        # input queue mocks
        container_services_mock.return_value.listen_to_input_queue.return_value = message

        ### WHEN ###
        main(config=mdf_parser_config)

        ### THEN ###
        assert('Invalid MDF path' in rec.message for rec in caplog.records if rec.levelname == 'ERROR')
        container_services_mock.return_value.send_message.assert_not_called()
        container_services_mock.return_value.delete_message.assert_not_called()
        container_services_mock.upload_file.assert_not_called()
        assert(graceful_exit_mock.call_count == 2)

    def test_mdf_parsing_download_error(self, mocker: MockerFixture, container_services_mock: Mock, graceful_exit_mock, caplog: LogCaptureFixture, mdf_parser_config: MdfParserConfig):
        ### GIVEN ###
        message = {
            'Body': json.dumps({
                '_id': recording_name,
                's3_path': s3_path
            }).replace('\"', '\''),
            'ReceiptHandle': 'receipt_handle'
        }

        # input queue mocks
        container_services_mock.return_value.listen_to_input_queue.return_value = message
        # downloader mocks
        container_services_mock.download_file.side_effect = Exception('Download error')

        ### WHEN ###
        main(config=mdf_parser_config)

        ### THEN ###
        assert('Download error' in rec.message for rec in caplog.records if rec.levelname == 'ERROR')
        container_services_mock.return_value.send_message.assert_not_called()
        container_services_mock.return_value.delete_message.assert_not_called()
        container_services_mock.upload_file.assert_not_called()
        assert(graceful_exit_mock.call_count == 2)
