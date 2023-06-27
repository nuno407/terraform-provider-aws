"""Test Labelling Bridge Controller"""

import json
from unittest.mock import Mock
import pytest

from labeling_bridge.controller import (ERROR_400_MSG, ERROR_500_MSG,
                                        init_controller)
from labeling_bridge.models.api import RequestImportJobDTO, RequestExportJobDTO


# pylint: disable=missing-function-docstring,missing-class-docstring

KOGNIC_EXPORT_URL = "/kognicExport"
KOGNIC_IMPORT_URL = "/kognicImport"


@pytest.fixture(name="mock_flask_client")
def flask_client():
    service = Mock()
    app = init_controller(service)
    app.testing = True
    with app.test_client() as test_client:
        return test_client, service


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


class TestStatusChecks:
    @pytest.mark.unit
    def test_alive(self, mock_flask_client):
        test_client = mock_flask_client[0]

        resp = test_client.get("/alive")

        assert resp.status_code == 200
        data = json.loads(resp.data)
        assert data["message"] == "Ok"
        assert data["statusCode"] == "200"

    @pytest.mark.unit
    def test_ready(self, mock_flask_client):
        test_client = mock_flask_client[0]

        resp = test_client.get("/ready")

        assert resp.status_code == 200
        data = json.loads(resp.data)
        assert data["message"] == "Ready"
        assert data["statusCode"] == "200"


class TestKognicExport:

    @pytest.mark.unit
    def test_post_kognic_export_200(self, mock_flask_client):
        # GIVEN
        test_client = mock_flask_client[0]
        service = mock_flask_client[1]
        service.kognic_export = Mock()
        test_obj = RequestExportJobDTO(**{
            "dataset": "dummy_dataset",
            "kognicProjectId": "dummy_project",
            "labellingType": ["Splines"],
            "labellingJobName": "dummy_batch",
            "labellingGuidelines": "instruction_1",
            "voxelExportMethod": "tag",
            "voxelTagToExport": "this_tag",
            "clientId": "abc",
                        "clientSecret": "123",
                        "filters": {},
                        "stages": []})

        # WHEN
        resp = test_client.post(KOGNIC_EXPORT_URL, json=test_obj.dict(by_alias=True))

        # THEN
        assert resp.status_code == 200
        data = json.loads(resp.data)
        assert data["statusCode"] == "200"
        assert data["message"] == "Data exported to Kognic!"
        service.kognic_export.assert_called_once_with(test_obj)

    @pytest.mark.unit
    def test_post_kognic_export_500(self, mock_flask_client):
        # GIVEN
        test_client = mock_flask_client[0]
        service = mock_flask_client[1]
        service.kognic_export = Mock(side_effect=Exception("Bad issue"))
        test_obj = RequestExportJobDTO(**{
            "dataset": "dummy_dataset",
            "kognicProjectId": "dummy_project",
            "labellingType": ["Splines"],
            "labellingJobName": "dummy_batch",
            "labellingGuidelines": "instruction_1",
            "voxelExportMethod": "tag",
            "voxelTagToExport": "this_tag",
            "clientId": "abc",
                        "clientSecret": "123",
                        "filters": {},
                        "stages": []})

        # WHEN
        resp = test_client.post(KOGNIC_EXPORT_URL, json=test_obj.dict(by_alias=True))

        # THEN
        data = json.loads(resp.data)
        assert data["statusCode"] == "500"
        assert data["message"] == "Internal Server Error"

    @pytest.mark.unit
    def test_post_kognic_export_400(self, mock_flask_client):
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
        print(resp.data)
        data = json.loads(resp.data)
        assert data["statusCode"] == "400"
        assert data["message"] == "Invalid or missing argument(s)"


class TestKognicImport:

    @pytest.mark.unit
    def test_post_kognic_import_200(self, mock_flask_client):
        # GIVEN
        test_client = mock_flask_client[0]
        service = mock_flask_client[1]
        service.kognic_import = Mock()
        test_obj = RequestImportJobDTO(**{"kognicProjectId": "dummy_project",
                                          "clientId": "abc",
                                          "clientSecret": "123",
                                          "dataset": "dummy_dataset",
                                          "labellingJobName": "dummy_batch"
                                          })

        # WHEN
        resp = test_client.post(KOGNIC_IMPORT_URL, json=test_obj.dict(by_alias=True))

        # THEN
        assert resp.status_code == 200
        data = json.loads(resp.data)
        assert data["statusCode"] == "200"
        assert data["message"] == "Data imported to Voxel!"
        service.kognic_import.assert_called_once_with(test_obj)

    @pytest.mark.unit
    def test_post_kognic_import_500(self, mock_flask_client):
        # GIVEN
        test_client = mock_flask_client[0]
        service = mock_flask_client[1]
        service.kognic_import = Mock(side_effect=Exception("Bad issue"))
        test_obj = RequestImportJobDTO(**{"kognicProjectId": "dummy_project",
                                          "clientId": "abc",
                                          "clientSecret": "123",
                                          "dataset": "dummy_dataset",
                                          "labellingJobName": "dummy_batch"
                                          })

        # WHEN
        resp = test_client.post(KOGNIC_IMPORT_URL, json=test_obj.dict(by_alias=True))

        # THEN
        assert resp.status_code == 500
        data = json.loads(resp.data)
        assert data["statusCode"] == "500"
        assert data["message"] == "Internal Server Error"

    @pytest.mark.unit
    def test_post_kognic_import_400(self, mock_flask_client):
        # GIVEN
        test_client = mock_flask_client[0]
        service = mock_flask_client[1]
        service.kognic_import = Mock(
            side_effect=ValueError("injected"))
        request_data = ""
        # WHEN
        resp = test_client.post(KOGNIC_IMPORT_URL, json=request_data)

        # THEN
        assert resp.status_code == 400
        data = json.loads(resp.data)
        assert data["statusCode"] == "400"
        assert data["message"] == "Invalid or missing argument(s)"
