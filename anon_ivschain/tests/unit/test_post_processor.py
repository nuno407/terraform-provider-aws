""" unit test module for anonymize post processor """
import os

from unittest import mock
from unittest.mock import Mock, call

import pytest

from base.aws.shared_functions import AWSServiceClients
from base.testing.mock_functions import get_container_services_mock
from basehandler.message_handler import InternalMessage, OperationalMessage
from anon_ivschain.post_processor import AnonymizePostProcessor


@pytest.mark.unit
class TestAnonymizePostProcessor():
    """ class for anonymize post processor unit testing """
    @mock.patch("builtins.open")
    @mock.patch("anon_ivschain.post_processor.os.stat")
    @mock.patch("anon_ivschain.post_processor.subprocess.check_call")
    @mock.patch("anon_ivschain.post_processor.os.remove")
    def test_run_anonymize_post_processor_1(

            self,
            os_remove: Mock,
            subprocess_check_call: Mock,
            os_stat: Mock,
            mock_open: Mock):
        """
        test the file format conversion from avi to mp4 using the run method
        with the removal of the output video before the file upload
        """
        # GIVEN
        aws_service = get_container_services_mock()
        aws_service.download_file = Mock(return_value=b"mocked_video_content")

        aws_clients = AWSServiceClients("mock_sqs", "mock_s3")

        anonymize_post_processor = AnonymizePostProcessor(
            aws_service, aws_clients)

        internal_message = OperationalMessage(
            InternalMessage.Status.PROCESSING_COMPLETED,
            "uid",
            "bucket",
            "test_dir/test_file.avi",
            "test_dir/test_file_anonymized.avi")

        # WHEN
        anonymize_post_processor.run(internal_message)

        # THEN

        # Assert call to subprocess
        os_stat.assert_called_once_with(AnonymizePostProcessor.INPUT_NAME)
        command, _ = subprocess_check_call.call_args
        command_str: str = str(command[0])

        assert AnonymizePostProcessor.INPUT_NAME in command_str
        assert AnonymizePostProcessor.OUTPUT_NAME in command_str
        assert "-y" in command_str
        assert "/usr/bin/ffmpeg" == command_str.split(" ", maxsplit=1)[0]

        # Assert calls to temporary files
        mock_open.assert_any_call(AnonymizePostProcessor.INPUT_NAME, "wb")
        mock_open.assert_any_call(AnonymizePostProcessor.OUTPUT_NAME, "rb")
        os_remove.assert_called_once_with(AnonymizePostProcessor.OUTPUT_NAME)

        # Assert uploaded files
        _args, _ = aws_service.upload_file.call_args_list[0]

        assert aws_clients.s3_client == _args[0]
        assert aws_service.anonymized_s3 == _args[2]
        assert os.path.splitext(internal_message.media_path)[
            0] + ".mp4" == _args[3]

    @mock.patch("builtins.open")
    @mock.patch("anon_ivschain.post_processor.os.stat")
    @mock.patch("anon_ivschain.post_processor.os.remove")
    def test_mocked_anonymize_post_processor(
            self,
            os_remove: Mock,
            os_stat: Mock,
            mock_open: Mock):
        """
        test the file format conversion from avi to mp4 using the run method
        """
        # GIVEN
        aws_service = get_container_services_mock()
        aws_service.download_file = Mock(return_value=b"mocked_video_content")

        aws_clients = AWSServiceClients("mock_sqs", "mock_s3")

        anonymize_post_processor = AnonymizePostProcessor(
            aws_service, aws_clients, mocked=True)

        internal_message = OperationalMessage(
            InternalMessage.Status.PROCESSING_COMPLETED,
            "uid",
            "bucket",
            "test_dir/test_file.avi",
            "test_dir/test_file_anonymized.avi")

        # WHEN
        anonymize_post_processor.run(internal_message)

        # THEN

        # Assert call to subprocess
        os_stat.assert_called_once_with(AnonymizePostProcessor.INPUT_NAME)

        # Assert calls to temporary files
        mock_open.assert_any_call(AnonymizePostProcessor.INPUT_NAME, "rb")
        mock_open.assert_any_call(AnonymizePostProcessor.OUTPUT_NAME, "wb")
        os_remove.assert_has_calls([
            call(AnonymizePostProcessor.INPUT_NAME),
            call(AnonymizePostProcessor.OUTPUT_NAME)
        ])

    @mock.patch("builtins.open")
    @mock.patch("anon_ivschain.post_processor.os.stat")
    @mock.patch("anon_ivschain.post_processor.subprocess.check_call")
    @mock.patch("anon_ivschain.post_processor.os.remove")
    def test_run_anonymize_post_processor_2(
            self,
            os_remove: Mock,
            subprocess_check_call: Mock,
            os_stat: Mock,
            mock_open: Mock):
        """
        test the file format conversion from avi to mp4 using the run method
        with the removal of the output video after the file upload

        """
        # GIVEN
        aws_service = get_container_services_mock()
        aws_service.download_file = Mock(return_value=b"mocked_video_content")

        aws_clients = AWSServiceClients("mock_sqs", "mock_s3")

        anonymize_post_processor = AnonymizePostProcessor(
            aws_service, aws_clients)

        internal_message = OperationalMessage(
            InternalMessage.Status.PROCESSING_COMPLETED,
            "uid",
            "bucket",
            "test_dir/test_file.avi",
            "test_dir/test_file_anonymized.avi")

        # WHEN
        anonymize_post_processor.run(internal_message)

        # THEN

        # Assert call to subprocess
        os_stat.assert_called_once_with(AnonymizePostProcessor.INPUT_NAME)
        command, _ = subprocess_check_call.call_args
        command_str: str = str(command[0])

        assert AnonymizePostProcessor.INPUT_NAME in command_str
        assert AnonymizePostProcessor.OUTPUT_NAME in command_str
        assert "-y" in command_str
        assert "/usr/bin/ffmpeg" == command_str.split(" ", maxsplit=1)[0]

        # Assert calls to temporary files
        mock_open.assert_any_call(AnonymizePostProcessor.INPUT_NAME, "wb")
        mock_open.assert_any_call(AnonymizePostProcessor.OUTPUT_NAME, "rb")

        # Assert uploaded files
        _args, _ = aws_service.upload_file.call_args_list[0]

        os_remove.assert_called_once_with("output_video.mp4")

        assert aws_clients.s3_client == _args[0]
        assert aws_service.anonymized_s3 == _args[2]
        assert os.path.splitext(internal_message.media_path)[
            0] + ".mp4" == _args[3]

    def test_run_anonymize_post_processor_wrong_format1(self):
        """
        test 1 for anonymize_post_processor run method with wrong file format
        File format differs from .avi
        """
        # GIVEN
        aws_service = get_container_services_mock()
        aws_service.download_file = Mock(return_value=b"mocked_video_content")

        aws_clients = AWSServiceClients("mock_sqs", "mock_s3")

        anonymize_post_processor = AnonymizePostProcessor(
            aws_service, aws_clients)

        internal_message = OperationalMessage(
            InternalMessage.Status.PROCESSING_COMPLETED,
            "uid",
            "bucket",
            "test_dir/test_file.none",
            "test_dir/test_file_anonymized.none")

        # WHEN
        anonymize_post_processor.run(internal_message)

        # THEN
        aws_service.download_file.assert_not_called()

    def test_run_anonymize_post_processor_wrong_format2(self):
        """
        test 2 for anonymize_post_processor run method with wrong file format
        File format not given
        """
        # GIVEN
        aws_service = get_container_services_mock()
        aws_service.download_file = Mock(return_value=b"mocked_video_content")

        aws_clients = AWSServiceClients("mock_sqs", "mock_s3")

        anonymize_post_processor = AnonymizePostProcessor(
            aws_service, aws_clients)

        internal_message = OperationalMessage(InternalMessage.Status.PROCESSING_COMPLETED,
                                              "uid", "bucket", "test_dir/test_file", "test_dir/test_file_anonymized")

        # WHEN
        anonymize_post_processor.run(internal_message)

        # THEN
        aws_service.download_file.assert_not_called()
