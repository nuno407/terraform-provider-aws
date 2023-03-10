from handler import GrafanaAPIHandler, GrafanaEnvironment
from unittest.mock import MagicMock

def test_export_dashboard():
    """ Test export dashboard. """
    mock_requests = MagicMock()
    mock_requests.get.return_value.status_code = 200
    mock_requests.get.return_value.json.return_value = {
        "dashboard": {
            "title": "Test Dashboard"
        }
    }
    handler = GrafanaAPIHandler(MagicMock(), mock_requests)
    dashboard = handler.export_dashboard("test")
    assert dashboard["title"] == "Test Dashboard"

def test_list_dashboard():
    """ Test search dashboard. """
    mock_requests = MagicMock()
    mock_requests.get.return_value.status_code = 200
    mock_requests.get.return_value.json.return_value = [{
        "title": "Test Dashboard"
    }]
    handler = GrafanaAPIHandler(MagicMock(), mock_requests)
    dashboard = handler.list_dashboards()
    assert dashboard[0]["title"] == "Test Dashboard"

def test_import_dashboard():
    mock_requests = MagicMock()
    mock_requests.post.return_value.status_code = 200
    mock_requests.post.return_value.json.return_value = {
        "url": "http://localhost:3000/d/test/test-dashboard"
    }
    gf_env = GrafanaEnvironment(
        base_url="http://localhost:3000",
        auth_token="test"
    )

    handler = GrafanaAPIHandler(gf_env, mock_requests)
    handler.import_dashboard({
        "title": "Test Dashboard"
    }, 1)
    mock_requests.post.assert_called_with(
        "http://localhost:3000/api/dashboards/db",
        headers={"Authorization": "Bearer test"},
        json={
            "dashboard": {
                "title": "Test Dashboard"
            },
            "folderId": 1,
            "message": "Imported from another Grafana instance.",
            "overwrite": True
        },
        timeout=5
    )
