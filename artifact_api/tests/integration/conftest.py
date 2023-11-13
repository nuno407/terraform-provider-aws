"""Configure tests"""
import os
import tempfile
os.environ["FIFTYONE_DATABASE_DIR"] = tempfile.TemporaryDirectory().name  # pylint: disable=consider-using-with
os.environ["FIFTYONE_DATABASE_ADMIN"] = "true"
os.environ["FIFTYONE_DO_NOT_TRACK"] = "true"
# Fiftyone launches a file database by itself when we import it without prior defining a database uri.
import fiftyone as _  # noqa # pylint: disable=wrong-import-position, wrong-import-order

import pytest  # noqa # pylint: disable=wrong-import-position, wrong-import-order
from fastapi.testclient import TestClient  # noqa # pylint: disable=wrong-import-position, wrong-import-order
from artifact_api.router import app  # noqa # pylint: disable=wrong-import-position, wrong-import-order


@pytest.fixture(scope="session")
def api_client() -> TestClient:
    """Test API client"""
    return TestClient(app)
