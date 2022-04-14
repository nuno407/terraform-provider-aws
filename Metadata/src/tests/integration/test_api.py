import json
import os
import sys
import mongomock
import pytest
from unittest.mock import MagicMock, Mock
from api.db import Persistence
from api.service import ApiService


sys.modules['api.config'] = MagicMock()
from api import controller

db_tables = { 'recording': 'recording', 'pipeline_exec': 'pipeline_exec', 'algo_output': 'algo_output'}
our_recording = 'our_recording'
other_recording = 'other_recording'
our_algo = 'what_we_are_looking_for'
other_algo = 'what_we_were_not_looking_for'

mockdb_client = mongomock.MongoClient()
recordings = mockdb_client.DB_data_ingestion.recording
pipeline_execs = mockdb_client.DB_data_ingestion.pipeline_exec
algo_outputs = mockdb_client.DB_data_ingestion.algo_output
persistence = Persistence(None, db_tables, False, mockdb_client)
s3mock = MagicMock()

controller.service = ApiService(persistence, s3mock)

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))
testrecordings = json.loads(open(os.path.join(__location__, 'test_data/recordings.json'), 'r').read())
testexecs = json.loads(open(os.path.join(__location__, 'test_data/pipeline_executions.json'), 'r').read())
testalgo = json.loads(open(os.path.join(__location__, 'test_data/algo_output.json'), 'r').read())
recordings.insert_many(testrecordings)
pipeline_execs.insert_many(testexecs)
algo_outputs.insert_one(testalgo)

@pytest.fixture
def client():
    controller.app.testing = True
    with controller.app.test_client() as client:
        return client

video_url_endpoint = '/getVideoUrl/foo/bar/baz'
def test_get_video_url(client):
    # GIVEN
    s3mock.generate_presigned_url = Mock(return_value='demoUrl')
    
    # WHEN
    resp = client.get(video_url_endpoint)

    # THEN
    assert resp.status_code == 200
    data = json.loads(resp.data)
    assert data['message'] == 'demoUrl'
    assert data['statusCode'] == '200'

    s3mock.generate_presigned_url.assert_called_once()
    args = s3mock.generate_presigned_url.call_args.args
    assert(args[0] == 'get_object')
    kwargs = s3mock.generate_presigned_url.call_args.kwargs
    params = kwargs['Params']
    assert(params['Bucket'] == 'foo')
    assert(params['Key'] == 'barbaz')


anon_video_url_endpoint = '/getAnonymizedVideoUrl/srxdriverpr1external07_rc_srx_qa_driverpr1_007_InteriorRecorder_1645517710980_1645519350202'
def test_get_video_url(client):
    # GIVEN
    s3mock.generate_presigned_url = Mock(return_value='demoUrl')
    
    # WHEN
    resp = client.get(anon_video_url_endpoint)

    # THEN
    assert resp.status_code == 200
    data = json.loads(resp.data)
    assert data['message'] == 'demoUrl'
    assert data['statusCode'] == '200'

    s3mock.generate_presigned_url.assert_called_once()
    args = s3mock.generate_presigned_url.call_args.args
    assert(args[0] == 'get_object')
    kwargs = s3mock.generate_presigned_url.call_args.kwargs
    params = kwargs['Params']
    assert(params['Bucket'] == 'qa-rcd-anonymized-video-files')
    assert(params['Key'] == 'Debug_Lync/srxdriverpr1external07_rc_srx_qa_driverpr1_007_InteriorRecorder_1645517710980_1645519350202_anonymized.mp4')