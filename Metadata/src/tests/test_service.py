import os
from unittest import TestCase
from unittest.mock import MagicMock, Mock
import json

from api.service import ApiService


db = MagicMock()
s3 = MagicMock()
service = ApiService(db, s3)

def test_create_video_url():
    # GIVEN
    file = 'bar.mp4'
    folder = 'foo/'
    bucket = 'baz'
    url = 'http://spam.egg/baz/'
    s3.generate_presigned_url = Mock(return_value=url)

    # WHEN
    return_value = service.create_video_url(bucket, folder, file)

    # THEN
    assert(return_value == url)
    s3.generate_presigned_url.assert_called_once()
    args = s3.generate_presigned_url.call_args.args
    assert(args[0] == 'get_object')
    kwargs = s3.generate_presigned_url.call_args.kwargs
    params = kwargs['Params']
    assert(params['Bucket'] == bucket)
    assert(params['Key'] == folder + file)

def test_create_anonymized_video_url():
    # GIVEN
    recording_id = 'qux'
    path = 'foobar.mp4'
    bucket = 'baz'
    url = 'http://spam.egg/baz/'
    db.get_algo_output = Mock(return_value={'output_paths': {'video': bucket + '/' + path}})
    s3.generate_presigned_url = Mock(return_value=url)

    # WHEN
    return_value = service.create_anonymized_video_url(recording_id)

    # THEN
    # db-part
    db.get_algo_output.assert_called_once_with('Anonymize', recording_id)
    # s3-part
    assert(return_value == url)
    s3.generate_presigned_url.assert_called_once()
    args = s3.generate_presigned_url.call_args.args
    assert(args[0] == 'get_object')
    kwargs = s3.generate_presigned_url.call_args.kwargs
    params = kwargs['Params']
    assert(params['Bucket'] == bucket)
    assert(params['Key'] == path)

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))
def test_get_table_data():
    # GIVEN
    jsonstr = open(os.path.join(__location__, 'test_data/recording_aggregation_response.json'), 'r').read()
    aggregation_result = json.loads(jsonstr)
    db.get_recording_list = Mock(return_value=(aggregation_result, 1, 1))

    # WHEN
    result, _, _ = service.get_table_data(10, 1)

    # THEN
    expectedstr = open(os.path.join(__location__, 'test_data/table_data_expected.json'), 'r').read()
    expected = json.loads(expectedstr)
    assert(expected == result)

def test_get_single_recording():
    # GIVEN
    jsonstr = open(os.path.join(__location__, 'test_data/recording_aggregation_response.json'), 'r').read()
    aggregation_result = json.loads(jsonstr)
    db.get_single_recording = Mock(side_effect=[aggregation_result[2], aggregation_result[3]])

    # WHEN
    result = service.get_single_recording("srxfut2internal20_rc_srx_qa_na_fut2_003_TrainingRecorder_1648835260452_1648835387254")

    # THEN
    expectedstr = open(os.path.join(__location__, 'test_data/single_recording_expected.json'), 'r').read()
    expected = json.loads(expectedstr)
    assert(result == expected)
    assert(db.get_single_recording.call_count == 2)
