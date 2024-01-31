# noqa # pylint: disable=wrong-import-position, wrong-import-order, wrong-import-position, redefined-outer-name, unused-argument
# autopep8: off
"""Configure tests"""
import os
import tempfile
import pytest
from motor.motor_asyncio import AsyncIOMotorClient
from httpx import AsyncClient
from kink import di
from base.testing.mock_functions import set_mock_aws_credentials
from base.testing.utils import get_abs_path

set_mock_aws_credentials()
os.environ["FIFTYONE_DATABASE_DIR"] = tempfile.TemporaryDirectory().name  # pylint: disable=consider-using-with
os.environ["FIFTYONE_DATABASE_ADMIN"] = "true"
os.environ["FIFTYONE_DO_NOT_TRACK"] = "true"

# Fiftyone launches a file database by itself when we import it without prior defining a database uri.
import fiftyone as fo

from artifact_api.router import app
from artifact_api.bootstrap import bootstrap_di
from artifact_api.voxel.voxel_config import VoxelConfig
# autopep8: on


DB_NAME = "DataIngestion"


@pytest.fixture()
def mongo_host() -> str:
    """
    Test API client

    Since fiftyone launches a mongodb instance by itself, it uses that database to preform the integration tests.
    This enviornment variable is not documented in fityone, and thus might change in the future.

    This uses an "hack" gather from the source code of the fiftyone library
    (https://github.com/voxel51/fiftyone/blob/26866c5a37e2dff83fee0f18bbde9f7153ff0e99/fiftyone/core/odm/database.py#L203) # pylint: disable=line-too-long
    It uses the FIFTYONE_PRIVATE_DATABASE_PORT to get the port from the internal DB

    """
    port = os.environ.get("FIFTYONE_PRIVATE_DATABASE_PORT")
    return f"127.0.0.1:{port}"


@pytest.fixture()
async def mongo_client(mongo_host: str) -> AsyncIOMotorClient:
    """Test API client"""
    client = AsyncIOMotorClient(mongo_host, tz_aware=True)
    await client.drop_database(DB_NAME)
    return client


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
def bootstrap_run(
        voxel_config: str,
        mongo_api_config: str,
        mongo_host: str):
    """Intilize dependency injection"""
    di.clear_cache()
    # asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # Clean Voxel and load default dataset
    fo.delete_datasets("*")
    config = VoxelConfig.load_yaml_config(voxel_config)
    dataset = fo.Dataset(config.dataset_mapping.default_dataset)
    dataset.tags = [config.dataset_mapping.tag]
    dataset.add_sample_field("data_privacy_document_id", ftype=fo.StringField)

    os.environ["TENANT_MAPPING_CONFIG_PATH"] = voxel_config
    os.environ["MONGODB_CONFIG"] = mongo_api_config
    os.environ["DATABASE_URI"] = mongo_host
    os.environ["DATABASE_NAME"] = DB_NAME

    bootstrap_di()


@pytest.fixture()
async def api_client(bootstrap_run) -> AsyncClient:
    """Test API client"""
    async with AsyncClient(app=app, base_url="http://test") as client:
        yield client
