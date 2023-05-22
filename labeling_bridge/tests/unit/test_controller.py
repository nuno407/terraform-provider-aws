"""Test Labelling Bridge Controller"""

import json
from unittest.mock import Mock
import pytest

from labeling_bridge.controller import (ERROR_400_MSG, ERROR_500_MSG,
                                        init_controller)


# pylint: disable=missing-function-docstring

@pytest.fixture(name="mock_flask_client")
def flask_client():
    service = Mock()
    app = init_controller(service)
    app.testing = True
    with app.test_client() as test_client:
        return test_client, service


@pytest.mark.unit
def test_alive(mock_flask_client):
    test_client = mock_flask_client[0]

    resp = test_client.get("/alive")

    assert resp.status_code == 200
    data = json.loads(resp.data)
    assert data["message"] == "Ok"
    assert data["statusCode"] == "200"


@pytest.mark.unit
def test_ready(mock_flask_client):
    test_client = mock_flask_client[0]

    resp = test_client.get("/ready")

    assert resp.status_code == 200
    data = json.loads(resp.data)
    assert data["message"] == "Ready"
    assert data["statusCode"] == "200"


def assert_returns_400(test_client, url):
    resp = test_client.get(url)

    assert resp.status_code == 400
    data = json.loads(resp.data)
    assert data["message"] == ERROR_400_MSG
    assert data["statusCode"] == "400"


def assert_returns_500(test_client, url):
    resp = test_client.get(url)

    assert resp.status_code == 500
    data = json.loads(resp.data)
    assert data["message"] == ERROR_500_MSG
    assert data["statusCode"] == "500"


KOGNIC_EXPORT_URL = "/kognicExport"


@pytest.mark.unit
def test_post_kognic_export_200(mock_flask_client):
    # GIVEN
    test_client = mock_flask_client[0]
    service = mock_flask_client[1]
    service.kognic_export = Mock()
    request_data = {"dataset": "dummy_dataset",
                    "kognicProjectId": "dummy_project",
                    "labellingType": ["Splines"],
                    "labellingJobName": "dummy_batch",
                    "labellingGuidelines": "instruction_1",
                    "voxelExportMethod": "tag",
                    "voxelTagToExport": "this_tag",
                    "clientId": "abc",
                    "clientSecret": "123",
                    "filters": {},
                    "stages": []}

    # WHEN
    resp = test_client.post(KOGNIC_EXPORT_URL, json=request_data)

    # THEN
    assert resp.status_code == 200
    data = json.loads(resp.data)
    assert data["statusCode"] == "200"
    assert data["message"] == "Data exported to Kognic!"
    service.kognic_export.assert_called_once_with(request_data)


@pytest.mark.unit
def test_post_kognic_export_500(mock_flask_client):
    # GIVEN
    test_client = mock_flask_client[0]
    service = mock_flask_client[1]
    service.kognic_export = Mock()

    # WHEN
    resp = test_client.post(KOGNIC_EXPORT_URL)

    # THEN
    assert resp.status_code == 500
    data = json.loads(resp.data)
    assert data["statusCode"] == "500"
    assert data["message"] == "Internal Server Error"


@pytest.mark.unit
def test_post_kognic_export_400(mock_flask_client):
    # GIVEN
    test_client = mock_flask_client[0]
    service = mock_flask_client[1]
    service.kognic_export = Mock(
        side_effect=ValueError("injected"))
    request_data = ""
    # WHEN
    resp = test_client.post(KOGNIC_EXPORT_URL, json=request_data)

    # THEN
    assert resp.status_code == 400
    data = json.loads(resp.data)
    assert data["statusCode"] == "400"
    assert data["message"] == "Invalid or missing argument(s)"
