"Integration tests for controller"
import pytest
from httpx import AsyncClient


class TestControllerEndpoints:
    "Integration tests for controller endpoints"

    @pytest.fixture
    def api_client_session(self, api_client: AsyncClient) -> AsyncClient:
        """Test API client"""
        return api_client

    @pytest.mark.asyncio
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
    async def test_post_endpoint_exist(self, api_client_session: AsyncClient, api_endpoint: str):
        """Test the an endpoint exists"""
        response = await api_client_session.post(api_endpoint)
        assert response.status_code in (200, 422)

    @pytest.mark.asyncio
    @pytest.mark.integration
    @pytest.mark.parametrize("api_endpoint", [
        ("/alive"),
    ])
    async def test_get_endpoint_exist(self, api_client_session: AsyncClient, api_endpoint: str):
        """Test that a get endpoint exists"""
        response = await api_client_session.get(api_endpoint)
        assert response.status_code == 200
