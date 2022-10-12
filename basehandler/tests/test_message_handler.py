"""Test message handler."""
import json
import queue
import typing
from unittest.mock import Mock

import pytest
import requests
from pytest_mock import MockFixture

from base.aws.shared_functions import AWSServiceClients
from base.testing.mock_functions import QUEUE_MOCK_LIST
from base.testing.mock_functions import get_container_services_mock
from basehandler.message_handler import MessageHandler
from basehandler.message_handler import NOOPPostProcessor
from basehandler.message_handler import RequestProcessingFailed


class TestMessageHandler():
    """TestMessageHandler class.

    Provide test methods and fixture for the message_handler
    """
    @typing.no_type_check
    @pytest.fixture
    def message_handler_fix(self, request: MockFixture) -> MessageHandler:
        """client MessageHandler fixture"""
        args = {
            "container_services": get_container_services_mock(),
            "consumer_name": "mock",
            "aws_clients": AWSServiceClients("mock_sqs", "mock_s3"),
            "internal_queue": queue.Queue(),
            "post_processor": NOOPPostProcessor()
        }

        # Allows to specify arguments in the parameters
        if hasattr(request, "param") and isinstance(request.param, dict):
            if "consumer_name" in request.param:
                args['consumer_name'] = request.param['consumer_name']
            if "post_processor" in request.param:
                args['post_processor'] = request.param['post_processor']
            if "internal_queue" in request.param:
                args['internal_queue'] = request.param['internal_queue']
            if "aws_clients" in request.param:
                args['aws_clients'] = request.param['aws_clients']
            if "container_services" in request.param:
                args['container_services'] = request.param['container_services']

        return MessageHandler(**args)

    def test_request_processing1(self, message_handler_fix: Mock):
        """Tests requesting process of a mp4 file.

        Args:
            message_handler_fix (MessageHandler): message handler fixture
        """
        response_mock = Mock()
        response_mock.status_code = 200
        requests.post = Mock(return_value=response_mock)

        body_json = """
        {
            "s3_path" : "video-raw-8956486794654.mp4"
        }
        """

        assert message_handler_fix.request_processing(body_json, 'anon')
        _, post_kwargs = requests.post.call_args
        assert len(post_kwargs['files'])
        assert post_kwargs['files'][0][0] == "video"
        assert 'uid' in post_kwargs['data']
        assert 'anon' in post_kwargs['data']['mode']
        assert "video-raw-8956486794654.mp4" in post_kwargs['data']['path']

    def test_request_processing2(self, message_handler_fix: Mock):
        """Tests requesting process of a jpg file.

        Args:
            message_handler_fix (MessageHandler): message handler fixture
        """
        response_mock = Mock()
        response_mock.status_code = 200
        requests.post = Mock(return_value=response_mock)

        body_json = """
        {
            "s3_path" : "image-raw.8956486794654.jpg"
        }
        """

        # WHEN
        return_val = message_handler_fix.request_processing(body_json, "anon")

        # THEN
        assert return_val
        _, post_kwargs = requests.post.call_args
        assert len(post_kwargs['files'])
        assert post_kwargs['files'][0][0] == "image"
        assert 'uid' in post_kwargs['data']
        assert 'anon' in post_kwargs['data']['mode']
        assert "image-raw.8956486794654.jpg" in post_kwargs['data']['path']

    def test_request_processing3(self, message_handler_fix: Mock):
        """Test request CHC processing of a bad filepath.

        Args:
            message_handler_fix (MessageHandler): message handler fixture
        """
        response_mock = Mock()
        response_mock.status_code = 200
        requests.post = Mock(return_value=response_mock)

        body_json = """
        {
            "s3_path" : "image-//raw.8956486794654,´«.jpg"
        }
        """

        assert message_handler_fix.request_processing(body_json, 'chc')

        _, post_kwargs = requests.post.call_args
        assert len(post_kwargs['files'])
        assert post_kwargs['files'][0][0] == "image"
        assert 'uid' in post_kwargs['data']
        assert 'chc' in post_kwargs['data']['mode']
        assert "image-//raw.8956486794654,´«.jpg" in post_kwargs['data']['path']

    def test_request_processing4(self, message_handler_fix: Mock):
        """Test request Anonymize processing of a bad filepath.

        Args:
            message_handler_fix (MessageHandler): message handler fixture
        """
        response_mock = Mock()
        response_mock.status_code = 200

        requests.post = Mock(return_value=response_mock)

        body_json = """
        {
            "s3_path" : "image-//raw.8956486794654,´«jpg.nothing"
        }
        """

        assert not message_handler_fix.request_processing(body_json, 'anon')

    @pytest.mark.parametrize('message_handler_fix', [dict(consumer_name="Anonymize")],
                             indirect=True)
    def test_update_processing1(self, message_handler_fix: Mock):
        """Test update processing steps.

        Args:
            message_handler_fix (MessageHandler): message handler fixture
        """
        dict_incomming_msg = {
            'processing_steps': ['Anonymize', 'CHC'],
            's3_path': 'Debug_Lync/deepsensation_rc_srx_prod_e798ed3795b6a072330b19468203529be4bdd821_TrainingRecorder_1659430874012_1659430990857.mp4',
            'data_status': 'received'
        }

        relay_list = message_handler_fix.update_processing(dict_incomming_msg)

        assert relay_list['processing_steps'][0] == 'CHC'
        assert relay_list['data_status'] == 'processing'
        assert relay_list['s3_path'] == 'Debug_Lync/deepsensation_rc_srx_prod_e798ed3795b6a072330b19468203529be4bdd821_TrainingRecorder_1659430874012_1659430990857.mp4'

    @pytest.mark.parametrize('message_handler_fix', [dict(consumer_name="Anonymize")], indirect=True)
    def test_update_processing2(self, message_handler_fix: Mock):
        """Test update processing steps.

        Args:
            message_handler_fix (MessageHandler): message handler fixture
        """
        dict_incomming_msg = {
            'processing_steps': ['Anonymize'],
            's3_path': 'Debug_Lync/deepsensation_rc_srx_prod_e798ed3795b6a072330b19468203529be4bdd821_TrainingRecorder_1659430874012_1659430990857.mp4',
            'data_status': 'received'
        }

        relay_list = message_handler_fix.update_processing(dict_incomming_msg)

        assert len(relay_list['processing_steps']) == 0
        assert relay_list['data_status'] == 'complete'
        assert relay_list['s3_path'] == 'Debug_Lync/deepsensation_rc_srx_prod_e798ed3795b6a072330b19468203529be4bdd821_TrainingRecorder_1659430874012_1659430990857.mp4'

    @pytest.mark.parametrize('message_handler_fix', [dict(consumer_name="CHC")], indirect=True)
    def test_update_processing3(self, message_handler_fix: Mock):
        """Test update processing steps.

        Args:
            message_handler_fix (MessageHandler): message handler fixture
        """
        dict_incomming_msg = {
            'processing_steps': ['new_algo', 'CHC'],
            's3_path': 'Debug_Lync/de.e,psensation_rc_srx_prod_e798ed3795b6a072330b19468203529be4bdd821_TrainingRecorder_1659430874012_1659430990857.mp4',
            'data_status': 'processing'
        }

        relay_list = message_handler_fix.update_processing(dict_incomming_msg)

        assert relay_list['processing_steps'][0] == 'new_algo'
        assert relay_list['data_status'] == 'processing'
        assert relay_list['s3_path'] == 'Debug_Lync/de.e,psensation_rc_srx_prod_e798ed3795b6a072330b19468203529be4bdd821_TrainingRecorder_1659430874012_1659430990857.mp4'

    @pytest.mark.parametrize('message_handler_fix', [dict(consumer_name="new_algo")], indirect=True)
    def test_update_processing4(self, message_handler_fix: Mock):
        """Test update processing steps.

        Args:
            message_handler_fix (MessageHandler): message handler fixture
        """
        dict_incomming_msg = {
            'processing_steps': ['new_algo', 'CHC', 'Anonymize'],
            's3_path': 'Debug_Lync/de.e,psensation_rc_srx_prod_e798ed3795b6a072330b19468203529be4bdd821_TrainingRecorder_1659430874012_1659430990857.mp4',
            'data_status': 'received'
        }

        relay_list = message_handler_fix.update_processing(dict_incomming_msg)

        assert relay_list['processing_steps'][0] == 'CHC'
        assert relay_list['processing_steps'][1] == 'Anonymize'
        assert relay_list['data_status'] == 'processing'
        assert relay_list['s3_path'] == 'Debug_Lync/de.e,psensation_rc_srx_prod_e798ed3795b6a072330b19468203529be4bdd821_TrainingRecorder_1659430874012_1659430990857.mp4'

    @pytest.mark.parametrize('message_handler_fix', [dict(consumer_name="Anonymize")], indirect=True)
    def test_handle_processing_output1(self, message_handler_fix: Mock):
        """Test update processing output.

        Args:
            message_handler_fix (MessageHandler): message handler fixture
        """
        incomming_msg = {
            "Body":    """{"processing_steps": ["new_algo","CHC","Anonymize"],
                        "s3_path": "Debug_Lync/de.e,psensation_rc_srx_prod_e798ed3795b6a072330b19468203529be4bdd821_TrainingRecorder_1659430874012_1659430990857.mp4",
                        "data_status": "received"}"""
        }

        dict_ivs_msg = {
            'uid': "uid",
            'status': "processing completed",
            'bucket': "container_services.anonymized_s3/chc_abc.a.b.e/d",
            'media_path': "jasj.,-kdhkajshd",
            'meta_path': "file_upload_path/path2/abc.opt"
        }

        container_services = message_handler_fix._MessageHandler__container_services

        message_handler_fix.handle_processing_output(
            incomming_msg, dict_ivs_msg)

        kwargs_metadata, _ = container_services.send_message.call_args_list[1]
        kwargs_next_queue, _ = container_services.send_message.call_args_list[0]

        message_metadata = json.loads(kwargs_metadata[2])
        message_next_queue = json.loads(kwargs_next_queue[2])

        # Next_queue
        assert kwargs_next_queue[0] == 'mock_sqs'  # Assert queue client
        # Assert next algo
        assert kwargs_next_queue[1] == QUEUE_MOCK_LIST['new_algo']
        assert message_next_queue['processing_steps'] == [
            "new_algo", "CHC", "Anonymize"]
        assert message_next_queue['s3_path'] == "Debug_Lync/de.e,psensation_rc_srx_prod_e798ed3795b6a072330b19468203529be4bdd821_TrainingRecorder_1659430874012_1659430990857.mp4"
        assert message_next_queue['data_status'] == 'processing'

        # Metadata_queue
        assert kwargs_metadata[0] == 'mock_sqs'  # Assert queque client
        # Assert next algo
        assert kwargs_metadata[1] == QUEUE_MOCK_LIST['Metadata']
        assert message_metadata['s3_path'] == "Debug_Lync/de.e,psensation_rc_srx_prod_e798ed3795b6a072330b19468203529be4bdd821_TrainingRecorder_1659430874012_1659430990857.mp4"
        assert message_metadata['data_status'] == 'processing'

        assert message_metadata['output']['bucket'] == "container_services.anonymized_s3/chc_abc.a.b.e/d"
        assert message_metadata['output']['media_path'] == "jasj.,-kdhkajshd"
        assert message_metadata['output']['meta_path'] == "file_upload_path/path2/abc.opt"

    @pytest.mark.parametrize('message_handler_fix', [dict(consumer_name="new_algo")], indirect=True)
    def test_handle_processing_output2(self, message_handler_fix: Mock):
        """Test update processing output.

        Args:
            message_handler_fix (MessageHandler): message handler fixture
        """
        incomming_msg = {
            "Body":    """{"processing_steps": ["new_algo","CHC","Anonymize"],
                        "s3_path": "Debug_Lync/de.e,psensation_rc_srx_prod_e798ed3795b6a072330b19468203529be4bdd821_TrainingRecorder_1659430874012_1659430990857.jpg",
                        "data_status": "processing"}"""
        }

        dict_ivs_msg = {
            'uid': "uid",
            'status': "processing done completed",
            'bucket': "container_services.anonymized_s3/chc_abc.a.b.e/d",
            'media_path': "jasj.,-kdhkajshd",
            'meta_path': "file_upload_path/path2/abc.opt"
        }

        container_services = message_handler_fix._MessageHandler__container_services

        message_handler_fix.handle_processing_output(
            incomming_msg, dict_ivs_msg)

        kwargs_metadata, _ = container_services.send_message.call_args_list[1]
        kwargs_next_queue, _ = container_services.send_message.call_args_list[0]

        message_metadata = json.loads(kwargs_metadata[2])
        message_next_queue = json.loads(kwargs_next_queue[2])

        # Next_queue
        assert kwargs_next_queue[0] == 'mock_sqs'  # Assert queue client
        # Assert next algo
        assert kwargs_next_queue[1] == QUEUE_MOCK_LIST['CHC']
        assert message_next_queue['s3_path'] == "Debug_Lync/de.e,psensation_rc_srx_prod_e798ed3795b6a072330b19468203529be4bdd821_TrainingRecorder_1659430874012_1659430990857.jpg"
        assert message_next_queue['data_status'] == 'processing'

        # Metadata_queue
        assert kwargs_metadata[0] == 'mock_sqs'  # Assert queue client
        # Assert next algo
        assert kwargs_metadata[1] == QUEUE_MOCK_LIST['Metadata']
        assert message_metadata['s3_path'] == "Debug_Lync/de.e,psensation_rc_srx_prod_e798ed3795b6a072330b19468203529be4bdd821_TrainingRecorder_1659430874012_1659430990857.jpg"
        assert message_metadata['data_status'] == 'processing'

        assert message_metadata['output']['bucket'] == "container_services.anonymized_s3/chc_abc.a.b.e/d"
        assert message_metadata['output']['media_path'] == "jasj.,-kdhkajshd"
        assert message_metadata['output']['meta_path'] == "file_upload_path/path2/abc.opt"

    @pytest.mark.parametrize('message_handler_fix', [dict(consumer_name="Anonymize")], indirect=True)
    def test_handle_processing_output3(self, message_handler_fix: Mock):
        """Test update processing output.

        Args:
            message_handler_fix (MessageHandler): message handler fixture
        """
        # GIVEN
        incomming_msg = {
            "Body":    """{"processing_steps": ["Anonymize"],
                        "s3_path": "Debug_Lync/de.e,psensation_rc_srx_prod_e798ed3795b6a072330b19468203529be4bdd821_TrainingRecorder_1659430874012_1659430990857.jpg",
                        "data_status": "processing"}"""
        }

        dict_ivs_msg = {
            'uid': "uid",
            'status': "processing completed",
            'bucket': "container_services.anonymized_s3/chc_abc.a.b.e/d",
            'media_path': "jasj.,-kdhkajshd",
            'meta_path': "file_upload_path/path2/abc.opt"
        }

        container_services = message_handler_fix._MessageHandler__container_services

        # WHEN
        message_handler_fix.handle_processing_output(
            incomming_msg, dict_ivs_msg)

        # THEN
        kwargs_metadata, _args = container_services.send_message.call_args_list[0]
        message_metadata = json.loads(kwargs_metadata[2])

        # Metadata_queue
        assert kwargs_metadata[0] == 'mock_sqs'  # Assert queque client
        # Assert next algo
        assert kwargs_metadata[1] == QUEUE_MOCK_LIST['Metadata']
        assert message_metadata['s3_path'] == "Debug_Lync/de.e,psensation_rc_srx_prod_e798ed3795b6a072330b19468203529be4bdd821_TrainingRecorder_1659430874012_1659430990857.jpg"
        assert message_metadata['data_status'] == 'complete'

        assert message_metadata['output']['bucket'] == "container_services.anonymized_s3/chc_abc.a.b.e/d"
        assert message_metadata['output']['media_path'] == "jasj.,-kdhkajshd"
        assert message_metadata['output']['meta_path'] == "file_upload_path/path2/abc.opt"

    def test_handle_incoming_message1(self, message_handler_fix: Mock):
        """Test handle incoming message.

        Args:
            message_handler_fix (MessageHandler): message handler fixture
        """
        message_handler_fix.request_processing = Mock(
            return_value=True)

        body_json = {'Body': """
        {
            'processing_steps': ['Anonymize'],
            's3_path': 'Debug_Lync/TrainingMultiSnapshot_TrainingMultiSnapshot-15f62ba7-489f-49c2-b936-d387124dc3d1_249_1659823752.mp4',
            'data_status': 'received'
        }
        """}

        message_handler_fix.handle_incoming_message(body_json, 'anon')
        _kwargs, _ = message_handler_fix.request_processing.call_args

        assert _kwargs[0] == body_json['Body']

    def test_handle_incoming_message2(self, message_handler_fix: Mock):
        """Test handle incoming message.

        Args:
            message_handler_fix (MessageHandler): message handler fixture
        """
        message_handler_fix.request_processing = Mock(return_value=False)

        body_json = {
            'Body': """
        {
            'processing_steps': ['Anonymize'],
            's3_path': 'Debug_Lync/TrainingMultiSnapshot_TrainingMultiSnapshot-15f62ba7-489f-49c2-b936-d387124dc3d1_249_1659823752.mp4',
            'data_status': 'received'
        }
        """
        }

        with pytest.raises(RuntimeError):
            message_handler_fix.handle_incoming_message(body_json, 'anon')

    def test_on_process1(self, message_handler_fix: Mock):
        """Test on process message.

        Args:
            message_handler_fix (MessageHandler): message handler fixture
        """
        sqs_queue_message = {
            'ReceiptHandle': '8765834756',
            'Body': """
                            {
                                'processing_steps': ['Anonymize'],
                                's3_path': 'Debug_Lync/TrainingMultiSnapshot_TrainingMultiSnapshot-15f62ba7-489f-49c2-b936-d387124dc3d1_249_1659823752.mp4',
                                'data_status': 'received'
                            }
                            """
        }
        internal_queue_message = {
            'chunk': [0, 1, 23, 4, 56],
            'path': "container_services.anonymized_s3/chc_abc.a.b.e/d",

            'msg_body': {
                'uid': "uid",
                'status': 'processing completed',
                'bucket': "container_services.anonymized_s3/chc_abc.a.b.e/d",
                'media_path': "jasj.,-kdhkajshd",
                'meta_path': "file_upload_path/path2/abc.opt"
            },
            'status': 'OK'
        }

        manager = Mock()

        message_handler_fix.handle_incoming_message = Mock(return_value=None)
        message_handler_fix.handle_processing_output = Mock(return_value=None)
        message_handler_fix._MessageHandler__container_services.listen_to_input_queue = Mock(
            return_value=sqs_queue_message)
        message_handler_fix._MessageHandler__internal_queue.get = Mock(
            return_value=internal_queue_message)

        manager.attach_mock(message_handler_fix.handle_incoming_message,
                            "handle_incoming_message")
        manager.attach_mock(
            message_handler_fix._MessageHandler__internal_queue.get, "internal_queue.get")
        manager.attach_mock(message_handler_fix.handle_processing_output,
                            "handle_processing_output")
        manager.attach_mock(
            message_handler_fix._MessageHandler__container_services.delete_message, "container_services.delete_message")

        message_handler_fix.on_process('chc')

        calls_list = manager.mock_calls

        assert calls_list[0][0] == "handle_incoming_message"
        assert calls_list[1][0] == "internal_queue.get"
        assert calls_list[2][0] == "handle_processing_output"
        assert calls_list[3][0] == "container_services.delete_message"

        message_handler_fix.handle_incoming_message.assert_called_with(
            sqs_queue_message, 'chc')
        message_handler_fix.handle_processing_output.assert_called_with(
            sqs_queue_message, internal_queue_message)
        message_handler_fix._MessageHandler__container_services.delete_message.assert_called_with(
            message_handler_fix._MessageHandler__aws_clients.sqs_client, sqs_queue_message['ReceiptHandle'])

    def test_on_process2(self, message_handler_fix: Mock):
        """Test on process message.

        Args:
            message_handler_fix (Mock): message handler fixture
        """
        sqs_queue_message = {
            'ReceiptHandle': '8765834756',
            'Body': """
                            {
                                'processing_steps': ['Anonymize'],
                                's3_path': 'Debug_Lync/TrainingMultiSnapshot_TrainingMultiSnapshot-15f62ba7-489f-49c2-b936-d387124dc3d1_249_1659823752.mp4',
                                'data_status': 'received'
                            }
                            """
        }
        internal_queue_message = {
            'chunk': None,
            'path': None,

            'msg_body': None,
            'status': 'ERROR'
        }

        # Mocks
        manager = Mock()

        message_handler_fix.handle_incoming_message = Mock(return_value=None)
        message_handler_fix.handle_processing_output = Mock(return_value=None)
        message_handler_fix._MessageHandler__container_services.listen_to_input_queue = Mock(
            return_value=sqs_queue_message)
        message_handler_fix._MessageHandler__internal_queue.get = Mock(
            return_value=internal_queue_message)

        manager.attach_mock(message_handler_fix.handle_incoming_message,
                            "handle_incoming_message")
        manager.attach_mock(
            message_handler_fix._MessageHandler__internal_queue.get, "internal_queue.get")
        manager.attach_mock(message_handler_fix.handle_processing_output,
                            "handle_processing_output")
        manager.attach_mock(
            message_handler_fix._MessageHandler__container_services.delete_message, "container_services.delete_message")

        with pytest.raises(RequestProcessingFailed):
            message_handler_fix.on_process('chc')

            calls_list = manager.mock_calls

            assert calls_list[0][0] == "handle_incoming_message"
            assert calls_list[1][0] == "internal_queue.get"

            with pytest.raises(RequestProcessingFailed):
                message_handler_fix.handle_incoming_message.assert_called_with(
                    sqs_queue_message, 'chc')
                message_handler_fix._MessageHandler__container_services.delete_message.assert_not_called()

    def test_on_process3(self, message_handler_fix: Mock):
        """Test on process message.

        Args:
            message_handler_fix (MessageHandler): message handler fixture
        """
        sqs_queue_message = {
            'ReceiptHandle': '8765834756',
            'Body': """
                {
                    'processing_steps': ['Anonymize'],
                    's3_path': 'Debug_Lync/TrainingMultiSnapshot_TrainingMultiSnapshot-15f62ba7-489f-49c2-b936-d387124dc3d1_249_1659823752.mp4',
                    'data_status': 'received'
                }
            """
        }
        internal_queue_message = {
            'chunk': None,
            'path': None,

            'msg_body': None,
            'status': 'unknown'
        }

        manager = Mock()

        message_handler_fix.handle_incoming_message = Mock(return_value=None)
        message_handler_fix.handle_processing_output = Mock(return_value=None)
        message_handler_fix._MessageHandler__container_services.listen_to_input_queue = Mock(
            return_value=sqs_queue_message)
        message_handler_fix._MessageHandler__internal_queue.get = Mock(
            return_value=internal_queue_message)

        manager.attach_mock(message_handler_fix.handle_incoming_message,
                            "handle_incoming_message")
        manager.attach_mock(
            message_handler_fix._MessageHandler__internal_queue.get, "internal_queue.get")
        manager.attach_mock(message_handler_fix.handle_processing_output,
                            "handle_processing_output")
        manager.attach_mock(
            message_handler_fix._MessageHandler__container_services.delete_message, "container_services.delete_message")

        with pytest.raises(RequestProcessingFailed):
            message_handler_fix.on_process('chc')

            calls_list = manager.mock_calls

            assert calls_list[0][0] == "handle_incoming_message"
            assert calls_list[1][0] == "internal_queue.get"

            with pytest.raises(RequestProcessingFailed):
                message_handler_fix.handle_incoming_message.assert_called_with(
                    sqs_queue_message, 'chc')
                message_handler_fix._MessageHandler__container_services.delete_message.assert_not_called()
