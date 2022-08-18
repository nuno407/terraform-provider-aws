from asyncio import subprocess
from unittest.mock import Mock, mock_open
from baseaws.mock_functions import get_container_services_mock
from baseaws.shared_functions import AWSServiceClients

from unittest import mock
from unittest.mock import Mock
import os

from post_processor import AnonymizePostProcessor

class TestAnonymizePostProcessor():

    @mock.patch("builtins.open")
    @mock.patch("post_processor.subprocess.Popen")
    @mock.patch("post_processor.subprocess.run")
    def test_run_anonymize_post_processor_1(self, subprocess_run: Mock, subprocess_open: Mock, open: Mock):
        # GIVEN
        aws_service = get_container_services_mock()
        aws_service.download_file = Mock(return_value=b'mocked_video_content')

        aws_clients = AWSServiceClients("mock_sqs","mock_s3")

        anonymize_post_processor = AnonymizePostProcessor(aws_service,aws_clients)

        message_body = {
            'media_path': 'test_dir/test_file_anonymized.mp4'
        }

        # WHEN
        anonymize_post_processor.run(message_body)

        # THEN

        # Assert call to subprocess
        subprocess_open.assert_called()
        subprocess_args, _ = subprocess_open.call_args
        list_command = subprocess_args[0]

        assert AnonymizePostProcessor.INPUT_NAME in list_command
        assert AnonymizePostProcessor.OUTPUT_NAME in list_command
        assert list_command[0] == 'ffmpeg'

        print(open.call_args_list)

        # Assert calls to temporary files
        open.assert_any_call(AnonymizePostProcessor.INPUT_NAME, 'wb')
        open.assert_any_call(AnonymizePostProcessor.OUTPUT_NAME, 'rb')

        # Assert uploaded files
        _args,_ = aws_service.upload_file.call_args_list[0]

        assert aws_clients.s3_client == _args[0]
        assert aws_service.anonymized_s3 == _args[2]
        assert os.path.splitext(message_body['media_path'])[0]+".mp4" == _args[3]



    @mock.patch("builtins.open")
    @mock.patch("post_processor.subprocess.Popen")
    @mock.patch("post_processor.subprocess.run")
    def test_run_anonymize_post_processor_2(self, subprocess_run: Mock, subprocess_open: Mock, open: Mock):
        # GIVEN
        aws_service = get_container_services_mock()
        aws_service.download_file = Mock(return_value=b'mocked_video_content')

        aws_clients = AWSServiceClients("mock_sqs","mock_s3")

        anonymize_post_processor = AnonymizePostProcessor(aws_service,aws_clients)

        message_body = {
            'media_path': 'test_dir/test_file.anonymized.avi'
        }

        # WHEN
        anonymize_post_processor.run(message_body)

        # THEN

        # Assert call to subprocess
        subprocess_open.assert_called()
        subprocess_args, _ = subprocess_open.call_args
        list_command = subprocess_args[0]

        assert AnonymizePostProcessor.INPUT_NAME in list_command
        assert AnonymizePostProcessor.OUTPUT_NAME in list_command
        assert list_command[0] == 'ffmpeg'

        # Assert calls to temporary files
        open.assert_any_call(AnonymizePostProcessor.INPUT_NAME, 'wb')
        open.assert_any_call(AnonymizePostProcessor.OUTPUT_NAME, 'rb')

        # Assert uploaded files
        _args,_ = aws_service.upload_file.call_args_list[0]

        assert aws_clients.s3_client == _args[0]
        assert aws_service.anonymized_s3 == _args[2]
        assert os.path.splitext(message_body['media_path'])[0]+".mp4" == _args[3]


    def test_run_anonymize_post_processor_wrong_format1(self):
        # GIVEN
        aws_service = get_container_services_mock()
        aws_service.download_file = Mock(return_value=b'mocked_video_content')

        aws_clients = AWSServiceClients("mock_sqs","mock_s3")

        anonymize_post_processor = AnonymizePostProcessor(aws_service,aws_clients)

        message_body = {
            'media_path': 'test_dir/test_file_anonymiz.ed.none'
        }

        # WHEN
        anonymize_post_processor.run(message_body)

        # THEN :D
        aws_service.download_file.assert_not_called()


        def test_run_anonymize_post_processor_wrong_format2(self):
            # GIVEN
            aws_service = get_container_services_mock()
            aws_service.download_file = Mock(return_value=b'mocked_video_content')

            aws_clients = AWSServiceClients("mock_sqs","mock_s3")

            anonymize_post_processor = AnonymizePostProcessor(aws_service,aws_clients)

        message_body = {
            'media_path': 'test_dir/test_file_anonymized'
        }

        # WHEN
        anonymize_post_processor.run(message_body)

        # THEN
        aws_service.download_file.assert_not_called()


