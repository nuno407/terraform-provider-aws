"Integration tests for controller"
from fastapi.testclient import TestClient
import pytest


class TestControllerEndpoints:
    "Integration tests for controller endpoints"

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
    def test_post_endpoint_exist(self, api_client: TestClient, api_endpoint: str):
        """Test the an endpoint exists"""
        response = api_client.post(api_endpoint)
        assert response.status_code in (200, 422)

    @pytest.mark.integration
    @pytest.mark.parametrize("api_endpoint", [
        ("/alive"),
    ])
    def test_get_endpoint_exist(self, api_client: TestClient, api_endpoint: str):
        """Test that a get endpoint exists"""
        response = api_client.get(api_endpoint)
        assert response.status_code == 200
