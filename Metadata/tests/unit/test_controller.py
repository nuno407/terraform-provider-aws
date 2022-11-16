"""Test API controller."""
import json
import sys
from functools import wraps
from typing import Callable
from unittest.mock import MagicMock, Mock

import pytest
from metadata.api.controller import (ERROR_400_MSG, ERROR_500_MSG,
                                     init_controller)
from pytest_mock import MockerFixture

sys.modules['api.config'] = MagicMock()


def mock_auth(api_method: Callable):
    @wraps(api_method)
    def auth_true(*args, **kwargs):
        return api_method(*args, **kwargs)
    return auth_true


@pytest.fixture(autouse=True)
def no_auth(mocker: MockerFixture):
    mocker.patch('metadata.api.controller.require_auth', mock_auth)


@pytest.fixture(name='flask_client')
def flask_client():
    service = Mock()
    app = init_controller(service)
    app.testing = True
    with app.test_client() as test_client:
        return test_client, service


@pytest.mark.unit
def test_alive(flask_client):
    test_client = flask_client[0]

    resp = test_client.get('/alive')

    assert resp.status_code == 200
    data = json.loads(resp.data)
    assert data['message'] == 'Ok'
    assert data['statusCode'] == '200'


@pytest.mark.unit
def test_ready(flask_client):
    test_client = flask_client[0]

    resp = test_client.get('/ready')

    assert resp.status_code == 200
    data = json.loads(resp.data)
    assert data['message'] == 'Ready'
    assert data['statusCode'] == '200'


VIDEO_URL = '/getVideoUrl/foo/bar/baz'


@pytest.mark.unit
def test_get_video_url_200(flask_client):
    test_client = flask_client[0]
    service = flask_client[1]

    service.create_video_url = Mock(return_value='demoUrl')

    resp = test_client.get(VIDEO_URL)

    assert resp.status_code == 200
    data = json.loads(resp.data)
    assert data['message'] == 'demoUrl'
    assert data['statusCode'] == '200'
    service.create_video_url.assert_called_once_with('foo', 'bar', 'baz')


@pytest.mark.unit
def test_get_video_url_400(flask_client):
    test_client = flask_client[0]
    service = flask_client[1]

    service.create_video_url = Mock(
        side_effect=LookupError('injected'))

    assert_returns_400(test_client, VIDEO_URL)


@pytest.mark.unit
def test_get_video_url_500(flask_client):
    test_client = flask_client[0]
    service = flask_client[1]

    service.create_video_url = Mock(
        side_effect=Exception('generic exception'))

    assert_returns_500(test_client, VIDEO_URL)


def assert_returns_400(test_client, url):
    resp = test_client.get(url)

    assert resp.status_code == 400
    data = json.loads(resp.data)
    assert data['message'] == ERROR_400_MSG
    assert data['statusCode'] == '400'


def assert_returns_500(test_client, url):
    resp = test_client.get(url)

    assert resp.status_code == 500
    data = json.loads(resp.data)
    assert data['message'] == ERROR_500_MSG
    assert data['statusCode'] == '500'


VIDEO_SIGNALS_URL = '/getVideoSignals/foo_video_id'


@pytest.mark.unit
def test_get_video_signals_200(flask_client):
    test_client = flask_client[0]
    service = flask_client[1]

    signals = {'CHC': {'0:00:00': {'CameraViewBlocked': 0.1}}}
    service.get_video_signals = Mock(return_value=signals)

    resp = test_client.get(VIDEO_SIGNALS_URL)

    assert resp.status_code == 200
    data = json.loads(resp.data)
    assert data['message'] == signals
    assert data['statusCode'] == '200'
    service.get_video_signals.assert_called_once_with(
        'foo_video_id')


@pytest.mark.unit
def test_get_video_signals_400(flask_client):
    test_client = flask_client[0]
    service = flask_client[1]

    service.get_video_signals = Mock(
        side_effect=LookupError('injected'))

    assert_returns_400(test_client, VIDEO_SIGNALS_URL)


@pytest.mark.unit
def test_get_video_signals_500(flask_client):
    test_client = flask_client[0]
    service = flask_client[1]

    service.get_video_signals = Mock(
        side_effect=Exception('generic exception'))

    assert_returns_500(test_client, VIDEO_SIGNALS_URL)


VIDEO_DESCRIPTION_URL = '/videoDescription/foo_video_id'


@pytest.mark.unit
def test_update_video_description_200(flask_client):
    test_client = flask_client[0]
    service = flask_client[1]

    description = 'Hello World!'
    description_obj = {'description': description}
    service.update_video_description = Mock()

    resp = test_client.put(VIDEO_DESCRIPTION_URL, json=description_obj)

    assert resp.status_code == 200
    service.update_video_description.assert_called_once_with(
        'foo_video_id', description)


@pytest.mark.unit
def test_update_video_description_400(flask_client):
    test_client = flask_client[0]
    service = flask_client[1]

    description = 'Hello World!'
    description_obj = {'description': description}
    service.update_video_description = Mock(
        side_effect=LookupError('injected'))

    resp = test_client.put(VIDEO_DESCRIPTION_URL, json=description_obj)
    assert resp.status_code == 400


@pytest.mark.unit
def test_update_video_description_500(flask_client):
    test_client = flask_client[0]
    service = flask_client[1]

    description = 'Hello World!'
    description_obj = {'description': description}
    service.update_video_description = Mock(
        side_effect=Exception('generic exception'))

    resp = test_client.put(VIDEO_DESCRIPTION_URL, json=description_obj)
    assert resp.status_code == 500


ALL_TABLE_DATA_URL = '/getTableData?page=2&size=10'


@pytest.mark.unit
def test_get_table_data_200(flask_client):
    test_client = flask_client[0]
    service = flask_client[1]

    recordings = [{'_id': 'recording1'}, {
        '_id': 'recording2', 'device_id': 'foo'}]

    service.get_table_data = Mock(return_value=(recordings, 12, 2))

    resp = test_client.get(ALL_TABLE_DATA_URL)

    assert resp.status_code == 200
    data = json.loads(resp.data)
    assert data['message'] == recordings
    assert data['pages'] == 2
    assert data['total'] == 12
    assert data['statusCode'] == '200'
    service.get_table_data.assert_called_once()


@pytest.mark.unit
def test_get_table_data_400(flask_client):
    test_client = flask_client[0]
    service = flask_client[1]

    service.get_table_data = Mock(
        side_effect=LookupError('injected'))

    assert_returns_400(test_client, ALL_TABLE_DATA_URL)


@pytest.mark.unit
def test_get_table_data_500(flask_client):
    test_client = flask_client[0]
    service = flask_client[1]

    service.get_table_data = Mock(
        side_effect=Exception('generic exception'))

    assert_returns_500(test_client, ALL_TABLE_DATA_URL)


SINGLE_TABLE_DATA_URL = '/getTableData/foo_video_id'


@pytest.mark.unit
def test_get_single_table_data_200(flask_client):
    test_client = flask_client[0]
    service = flask_client[1]

    recording = {'_id': 'foo_video_id', 'device_id': 'bar'}
    service.get_single_recording = Mock(return_value=recording)

    resp = test_client.get(SINGLE_TABLE_DATA_URL)

    assert resp.status_code == 200
    data = json.loads(resp.data)
    assert data['message'] == recording
    assert data['statusCode'] == '200'
    service.get_single_recording.assert_called_once_with(
        'foo_video_id')


@pytest.mark.unit
def test_get_single_table_data_400(flask_client):
    test_client = flask_client[0]
    service = flask_client[1]

    service.get_single_recording = Mock(
        side_effect=LookupError('injected'))

    assert_returns_400(test_client, SINGLE_TABLE_DATA_URL)


@pytest.mark.unit
def test_get_single_table_data_500(flask_client):
    test_client = flask_client[0]
    service = flask_client[1]

    service.get_single_recording = Mock(
        side_effect=Exception('generic exception'))

    assert_returns_500(test_client, SINGLE_TABLE_DATA_URL)


ANONYMIZED_VIDEO_URL = '/getAnonymizedVideoUrl/foo_video_id'


@pytest.mark.unit
def test_get_anonymized_video_url_200(flask_client):
    test_client = flask_client[0]
    service = flask_client[1]

    url = 'demo_url'
    service.create_anonymized_video_url = Mock(return_value=url)

    resp = test_client.get(ANONYMIZED_VIDEO_URL)

    assert resp.status_code == 200
    data = json.loads(resp.data)
    assert data['message'] == url
    assert data['statusCode'] == '200'
    service.create_anonymized_video_url.assert_called_once_with(
        'foo_video_id')


@pytest.mark.unit
def test_get_anonymized_video_url_400(flask_client):
    test_client = flask_client[0]
    service = flask_client[1]

    service.create_anonymized_video_url = Mock(
        side_effect=LookupError('injected'))

    assert_returns_400(test_client, ANONYMIZED_VIDEO_URL)


@pytest.mark.unit
def test_get_anonymized_video_url_500(flask_client):
    test_client = flask_client[0]
    service = flask_client[1]

    service.create_anonymized_video_url = Mock(
        side_effect=Exception('generic exception'))

    assert_returns_500(test_client, ANONYMIZED_VIDEO_URL)
