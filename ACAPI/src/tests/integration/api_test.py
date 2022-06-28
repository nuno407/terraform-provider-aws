"""
This file has the responsability of
doing the API the integration API tests
to ensure that everything is working in
the cluster.
"""

import io
from time import sleep
import pytest
from unittest.mock import MagicMock, Mock, PropertyMock
from unittest import mock
import api
import json
from baseaws.shared_functions import ContainerServices



@pytest.fixture
def client():
    api.app.testing = True
    with api.app.test_client() as client:
        return client

def init_container_services(self):
        # Config variables
        ## Important note for mock init methods:
        ## All private variables need to be prefixed with _ClassName when set from here
        ## e.g. instead of setting self.__queues, we need to set self._ContainerServices__queues
        self._ContainerServices__queues = {'list': {'API_Anonymize': 'API_Anonymize'}, 'input': ""}
        self._ContainerServices__msp_steps = {}
        self._ContainerServices__db_tables = {}
        self._ContainerServices__s3_buckets = {'raw': "raw", 'anonymized': "anonymized"}
        self._ContainerServices__s3_ignore = {'raw': "raw", 'anonymized': "anonymized"}
        self._ContainerServices__sdr_folder = {}
        self._ContainerServices__sdr_blacklist = {}
        self._ContainerServices__rcc_info = {}
        self._ContainerServices__ivs_api = {}
        self._ContainerServices__docdb_config = {}

        # Container info
        self._ContainerServices__container = {'name': "ACAPI", 'version': "X"}

        # Time format
        self._ContainerServices__time_format = "%Y-%m-%dT%H:%M:%S.%fZ"

@mock.patch('baseaws.shared_functions.ContainerServices.__init__', init_container_services)
def test_anonymization_snapshot(client): 
    # GIVEN
    api.container_services = ContainerServices()

    api.s3_client = MagicMock()
    api.s3_client.put_object = Mock()
    api.sqs_client = MagicMock()
    api.sqs_client.send_message = Mock()
    queue_url = 'http://testexample.com/'
    api.sqs_client.get_queue_url = Mock(return_value={'QueueUrl': queue_url})
    
    # WHEN
    data = {'uid':'bar', 'path':'foo.jpeg'}
    data = {key: str(value) for key, value in data.items() }
    data['file'] = (io.BytesIO(b"test"), "foo.jpeg")
    resp = client.post("/anonymized", data = data, content_type='multipart/form-data')
    sleep(1)

    #THEN
    assert resp.status_code == 202
    
    putobject_kwargs = api.s3_client.put_object.call_args.kwargs
    assert(putobject_kwargs['Bucket'] == 'anonymized')
    assert(putobject_kwargs['Key'] == 'foo_anonymized.jpeg')
    assert(putobject_kwargs['ContentType'] == 'image/jpeg')
    assert(putobject_kwargs['ServerSideEncryption'] == 'aws:kms')
    assert(putobject_kwargs['Body'] == b"test")

    get_queue_url_kwargs = api.sqs_client.get_queue_url.call_args.kwargs
    assert(get_queue_url_kwargs['QueueName'] == 'API_Anonymize')

    send_message_kwargs = api.sqs_client.send_message.call_args.kwargs
    assert(send_message_kwargs['QueueUrl'] == queue_url)
    message_body = json.loads(send_message_kwargs['MessageBody'].replace('\'', '\"'))
    assert(message_body['uid'] == 'bar')
    assert(message_body['status'] == 'processing completed')
    assert(message_body['bucket'] == 'anonymized')
    assert(message_body['media_path'] == 'foo_anonymized.jpeg')
    
@mock.patch('baseaws.shared_functions.ContainerServices.__init__', init_container_services)
def test_anonymization_video(client): 
    # GIVEN
    api.container_services = ContainerServices()

    api.s3_client = MagicMock()
    api.s3_client.put_object = Mock()
    api.sqs_client = MagicMock()
    api.sqs_client.send_message = Mock()
    queue_url = 'http://testexample.com/'
    api.sqs_client.get_queue_url = Mock(return_value={'QueueUrl': queue_url})
    
    # WHEN
    data = {'uid':'bar', 'path':'foo.mp4'}
    data = {key: str(value) for key, value in data.items() }
    data['file'] = (io.BytesIO(b"test"), "foo.mp4")
    resp = client.post("/anonymized", data = data, content_type='multipart/form-data')
    sleep(1)

    #THEN
    assert resp.status_code == 202
    
    putobject_kwargs = api.s3_client.put_object.call_args.kwargs
    assert(putobject_kwargs['Bucket'] == 'anonymized')
    assert(putobject_kwargs['Key'] == 'foo_anonymized.mp4')
    assert(putobject_kwargs['ContentType'] == 'video/mp4')
    assert(putobject_kwargs['ServerSideEncryption'] == 'aws:kms')
    assert(putobject_kwargs['Body'] == b"test")

    get_queue_url_kwargs = api.sqs_client.get_queue_url.call_args.kwargs
    assert(get_queue_url_kwargs['QueueName'] == 'API_Anonymize')

    send_message_kwargs = api.sqs_client.send_message.call_args.kwargs
    assert(send_message_kwargs['QueueUrl'] == queue_url)
    message_body = json.loads(send_message_kwargs['MessageBody'].replace('\'', '\"'))
    assert(message_body['uid'] == 'bar')
    assert(message_body['status'] == 'processing completed')
    assert(message_body['bucket'] == 'anonymized')
    assert(message_body['media_path'] == 'foo_anonymized.mp4')
    