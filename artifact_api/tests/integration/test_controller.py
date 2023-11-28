"Integration tests for controller"
import pytest
from fastapi.testclient import TestClient


class TestControllerEndpoints:
    "Integration tests for controller endpoints"

    @pytest.fixture
    def api_client_session(self, api_client: TestClient) -> TestClient:
        """Test API client"""
        return api_client

    @pytest.mark.integration
    @pytest.mark.parametrize("api_endpoint", [
        ("/ridecare/signals/video"),
        ("/ridecare/signals/snapshot"),
        ("/ridecare/video"),
        ("/ridecare/snapshots"),
        ("/ridecare/imu/video"),
        ("/ridecare/pipeline/anonymize/video"),
        ("/ridecare/pipeline/anonymize/snapshot"),
        ("/ridecare/pipeline/chc/video"),
        ("/ridecare/pipeline/status"),
        ("/ridecare/operator"),
        ("/ridecare/event")
    ])
    def test_post_endpoint_exist(self, api_client_session: TestClient, api_endpoint: str):
        """Test the an endpoint exists"""
        response = api_client_session.post(api_endpoint)
        assert response.status_code in (200, 422)

    @pytest.mark.integration
    @pytest.mark.parametrize("api_endpoint", [
        ("/alive"),
    ])
    def test_get_endpoint_exist(self, api_client_session: TestClient, api_endpoint: str):
        """Test that a get endpoint exists"""
        response = api_client_session.get(api_endpoint)
        assert response.status_code == 200
