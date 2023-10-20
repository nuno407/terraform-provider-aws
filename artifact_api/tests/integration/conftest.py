"""Configure tests"""
import pytest
from fastapi.testclient import TestClient
from artifact_api.router import app


@pytest.fixture(scope="session")
def api_client() -> TestClient:
    """Test API client"""
    return TestClient(app)
