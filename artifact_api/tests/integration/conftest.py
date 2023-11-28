# noqa # pylint: disable=wrong-import-position, wrong-import-order, wrong-import-position, redefined-outer-name, unused-argument
# autopep8: off
"""Configure tests"""
from fastapi.testclient import TestClient
import os
import tempfile
import pytest
from motor.motor_asyncio import AsyncIOMotorClient
from mongomock_motor import AsyncMongoMockClient
from kink import di
from unittest.mock import patch, Mock

os.environ["FIFTYONE_DATABASE_DIR"] = tempfile.TemporaryDirectory().name  # pylint: disable=consider-using-with
os.environ["FIFTYONE_DATABASE_ADMIN"] = "true"
os.environ["FIFTYONE_DO_NOT_TRACK"] = "true"
# Fiftyone launches a file database by itself when we import it without prior defining a database uri.
import fiftyone as fo

from base.testing.utils import get_abs_path
from artifact_api.router import app
from artifact_api.bootstrap import bootstrap_di
from artifact_api.voxel.voxel_config import VoxelConfig
# autopep8: on


DB_NAME = "DataIngestion"


@pytest.fixture()
def mongo_client() -> AsyncMongoMockClient:
    """Test API client"""
    return AsyncMongoMockClient()


@pytest.fixture()
def voxel_config(request) -> str:
    """Test API client"""
    filename = "voxel_config.yml"
    if hasattr(request, "param"):
        filename = request.param
    path = get_abs_path(__file__, f"test_data/configs/{filename}")
    return path


@pytest.fixture()
def mongo_api_config(request) -> str:
    """Test API client"""
    filename = "mongo_config.yml"
    if hasattr(request, "param"):
        filename = request.param
    path = get_abs_path(__file__, f"test_data/configs/{filename}")
    return path


@pytest.fixture()
@patch("artifact_api.bootstrap.create_mongo_client")
def bootstrap_run(
        create_mongo_client: Mock,
        voxel_config: str,
        mongo_api_config: str,
        mongo_client: AsyncIOMotorClient):
    """Intilize dependency injection"""
    di.clear_cache()

    # Clean Voxel and load default dataset
    fo.delete_datasets("*")
    config = VoxelConfig.load_yaml_config(voxel_config)
    dataset = fo.Dataset(config.dataset_mapping.default_dataset)
    dataset.tags = [config.dataset_mapping.tag]
    dataset.add_sample_field("data_privacy_document_id", ftype=fo.StringField)

    create_mongo_client.return_value = mongo_client

    os.environ["TENANT_MAPPING_CONFIG_PATH"] = voxel_config
    os.environ["MONGODB_CONFIG"] = mongo_api_config
    os.environ["DATABASE_URI"] = "mongodb://some_uri"
    os.environ["DATABASE_NAME"] = DB_NAME

    bootstrap_di()


@pytest.fixture()
def api_client(bootstrap_run) -> TestClient:
    """Test API client"""
    return TestClient(app)
