import json
import sys
from unittest.mock import MagicMock, Mock
import pytest

sys.modules['api.config'] = MagicMock()
from metadata.api import controller
from metadata.api.controller import ERROR_400_MSG, ERROR_500_MSG

@pytest.fixture
def client():
    controller.app.testing = True
    with controller.app.test_client() as client:
        return client

@pytest.mark.unit
def test_alive(client):
    # WHEN
    resp = client.get('/alive')

    # THEN
    assert resp.status_code == 200
    data = json.loads(resp.data)
    assert data['message'] == 'Ok'
    assert data['statusCode'] == '200'

@pytest.mark.unit
def test_ready(client):
    # WHEN
    resp = client.get('/ready')

    # THEN
    assert resp.status_code == 200
    data = json.loads(resp.data)
    assert data['message'] == 'Ready'
    assert data['statusCode'] == '200'

video_url = '/getVideoUrl/foo/bar/baz'
@pytest.mark.unit
def test_get_video_url_200(client):
    # GIVEN
    controller.service.create_video_url = Mock(return_value='demoUrl')

    # WHEN
    resp = client.get(video_url)

    # THEN
    assert resp.status_code == 200
    data = json.loads(resp.data)
    assert data['message'] == 'demoUrl'
    assert data['statusCode'] == '200'
    controller.service.create_video_url.assert_called_once_with('foo','bar','baz')

@pytest.mark.unit
def test_get_video_url_400(client):
    # GIVEN
    controller.service.create_video_url = Mock(side_effect=LookupError('injected'))

    # WHEN-THEN
    assert_returns_400(client, video_url)

@pytest.mark.unit
def test_get_video_url_500(client):
    # GIVEN
    controller.service.create_video_url = Mock(side_effect=Exception('generic exception'))

    # WHEN-THEN
    assert_returns_500(client, video_url)

@pytest.mark.unit
def assert_returns_400(client, url):
    # WHEN
    resp = client.get(url)

    # THEN
    assert resp.status_code == 400
    data = json.loads(resp.data)
    assert data['message'] == ERROR_400_MSG
    assert data['statusCode'] == '400'

@pytest.mark.unit
def assert_returns_500(client, url):
    # WHEN
    resp = client.get(url)

    # THEN
    assert resp.status_code == 500
    data = json.loads(resp.data)
    assert data['message'] == ERROR_500_MSG
    assert data['statusCode'] == '500'

video_signals_url = '/getVideoSignals/foo_video_id'
@pytest.mark.unit
def test_get_video_signals_200(client):
    # GIVEN
    signals = {'CHC':{'0:00:00':{'CameraViewBlocked': 0.1}}}
    controller.service.get_video_signals = Mock(return_value=signals)

    # WHEN
    resp = client.get(video_signals_url)

    # THEN
    assert resp.status_code == 200
    data = json.loads(resp.data)
    assert data['message'] == signals
    assert data['statusCode'] == '200'
    controller.service.get_video_signals.assert_called_once_with('foo_video_id')

@pytest.mark.unit
def test_get_video_signals_400(client):
    # GIVEN
    controller.service.get_video_signals = Mock(side_effect=LookupError('injected'))

    # WHEN-THEN
    assert_returns_400(client, video_signals_url)

@pytest.mark.unit
def test_get_video_signals_500(client):
    # GIVEN
    controller.service.get_video_signals = Mock(side_effect=Exception('generic exception'))

    # WHEN-THEN
    assert_returns_500(client, video_signals_url)

video_description_url = '/videoDescription/foo_video_id'
@pytest.mark.unit
def test_update_video_description_200(client):
    # GIVEN
    description = 'Hello World!'
    description_obj = {'description': description}
    controller.service.update_video_description = Mock()

    # WHEN
    resp = client.put(video_description_url, json = description_obj)

    # THEN
    assert resp.status_code == 200
    controller.service.update_video_description.assert_called_once_with('foo_video_id', description)

@pytest.mark.unit
def test_update_video_description_400(client):
    # GIVEN
    description = 'Hello World!'
    description_obj = {'description': description}
    controller.service.update_video_description = Mock(side_effect=LookupError('injected'))

    # WHEN
    resp = client.put(video_description_url, json = description_obj)
    # THEN
    assert resp.status_code == 400

@pytest.mark.unit
def test_update_video_description_500(client):
    # GIVEN
    description = 'Hello World!'
    description_obj = {'description': description}
    controller.service.update_video_description = Mock(side_effect=Exception('generic exception'))

    # WHEN
    resp = client.put(video_description_url, json = description_obj)
    #THEN
    assert resp.status_code == 500

all_table_data_url = '/getTableData?page=2&size=10'
@pytest.mark.unit
def test_get_table_data_200(client):
    # GIVEN
    recordings = [{'_id': 'recording1'}, {'_id': 'recording2', 'device_id': 'foo'}]
    controller.service.get_table_data = Mock(return_value=(recordings, 12, 2))

    # WHEN
    resp = client.get(all_table_data_url)

    # THEN
    assert resp.status_code == 200
    data = json.loads(resp.data)
    assert data['message'] == recordings
    assert data['pages'] == 2
    assert data['total'] == 12
    assert data['statusCode'] == '200'
    controller.service.get_table_data.assert_called_once()

@pytest.mark.unit
def test_get_table_data_400(client):
    # GIVEN
    controller.service.get_table_data = Mock(side_effect=LookupError('injected'))

    # WHEN-THEN
    assert_returns_400(client, all_table_data_url)

@pytest.mark.unit
def test_get_table_data_500(client):
    # GIVEN
    controller.service.get_table_data = Mock(side_effect=Exception('generic exception'))

    # WHEN-THEN
    assert_returns_500(client, all_table_data_url)

single_table_data_url = '/getTableData/foo_video_id'
@pytest.mark.unit
def test_get_single_table_data_200(client):
    # GIVEN
    recording = {'_id': 'foo_video_id', 'device_id': 'bar'}
    controller.service.get_single_recording = Mock(return_value=recording)

    # WHEN
    resp = client.get(single_table_data_url)

    # THEN
    assert resp.status_code == 200
    data = json.loads(resp.data)
    assert data['message'] == recording
    assert data['statusCode'] == '200'
    controller.service.get_single_recording.assert_called_once_with('foo_video_id')

@pytest.mark.unit
def test_get_single_table_data_400(client):
    # GIVEN
    controller.service.get_single_recording = Mock(side_effect=LookupError('injected'))

    # WHEN-THEN
    assert_returns_400(client, single_table_data_url)

@pytest.mark.unit
def test_get_single_table_data_500(client):
    # GIVEN
    controller.service.get_single_recording = Mock(side_effect=Exception('generic exception'))

    # WHEN-THEN
    assert_returns_500(client, single_table_data_url)

anonymized_video_url_url = '/getAnonymizedVideoUrl/foo_video_id'
@pytest.mark.unit
def test_get_anonymized_video_url_200(client):
    # GIVEN
    url = 'demo_url'
    controller.service.create_anonymized_video_url = Mock(return_value=url)

    # WHEN
    resp = client.get(anonymized_video_url_url)

    # THEN
    assert resp.status_code == 200
    data = json.loads(resp.data)
    assert data['message'] == url
    assert data['statusCode'] == '200'
    controller.service.create_anonymized_video_url.assert_called_once_with('foo_video_id')

@pytest.mark.unit
def test_get_anonymized_video_url_400(client):
    # GIVEN
    controller.service.create_anonymized_video_url = Mock(side_effect=LookupError('injected'))

    # WHEN-THEN
    assert_returns_400(client, anonymized_video_url_url)

@pytest.mark.unit
def test_get_anonymized_video_url_500(client):
    # GIVEN
    controller.service.create_anonymized_video_url = Mock(side_effect=Exception('generic exception'))

    # WHEN-THEN
    assert_returns_500(client, anonymized_video_url_url)
