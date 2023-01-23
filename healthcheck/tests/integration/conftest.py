"""Integration tests configuration module."""
import codecs
import functools
import json
import os

import boto3
import mongomock
import pytest
from bson import json_util
from moto import mock_s3
from mypy_boto3_s3 import S3Client
from unittest.mock import MagicMock

from base.aws.container_services import ContainerServices
from healthcheck.controller.aws_s3 import S3Controller
from healthcheck.controller.db import DatabaseController
from healthcheck.controller.voxel_fiftyone import VoxelFiftyOneController
from healthcheck.database import NoSQLDBConfiguration
from healthcheck.model import S3Params
from healthcheck.mongo import MongoDBClient
from healthcheck.schema.validator import JSONSchemaValidator
from healthcheck.voxel_client import VoxelDataset, VoxelEntriesGetter

CURRENT_LOCATION = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))
MONGO_DATA = os.path.join(CURRENT_LOCATION, "data", "mongo_data")
MONGO_DB_NAME = 'DataIngestion'

S3_DATA = os.path.join(CURRENT_LOCATION, "data", "s3_data")
VOXEL_DATA = os.path.join(CURRENT_LOCATION, "data", "voxel_data")


# inspired by pytest-mongodb plugin https://github.com/mdomke/pytest-mongodb/blob/develop/pytest_mongodb/plugin.py
_memoized_cache: dict = {}


def get_document_from_fixture_by_id(collection_file: str, id_field: str, id_value: str) -> dict:
    """get_document_from_fixture_by_id.

    Args:
        collection_file (str): collection fixture file under ``mongo_data`` with extension format
        id_field (str): identifier field
        id_value (str): identifier value

    Raises:
        ValueError: raised if id_value not found

    Returns:
        dict: filtered document from fixture file
    """
    full_fixture_path = get_collection_full_fixture_path(collection_file)
    collection_documents = load_collection_fixture(full_fixture_path)
    filtered_documents = list(
        filter(lambda doc: doc[id_field] == id_value, collection_documents))
    if len(filtered_documents):
        return filtered_documents[0]
    else:
        raise ValueError(
            "id_value %s not found for given id_field %s", id_value, id_field)


def get_collection_full_fixture_path(collection_file: str) -> str:
    """get_collection_full_fixture_path.

    Args:
        collection_file (str): collection file with extension format

    Returns:
        str: full path to collection fixture file
    """
    return os.path.join(MONGO_DATA, collection_file)


def load_collection_fixture(full_file_path: str) -> dict:
    """load_collection_fixture.

    Loads fixtures from file if not present in cache.

    Args:
        full_file_path (str): full path

    Returns:
        dict: all documents loaded
    """
    global _memoized_cache
    bson_loader = functools.partial(
        json.load, object_hook=json_util.object_hook)
    if full_file_path in _memoized_cache:
        docs = _memoized_cache[full_file_path]
    else:
        with codecs.open(full_file_path, encoding="utf-8") as file_pointer:
            docs = bson_loader(file_pointer)
            _memoized_cache[full_file_path] = docs
    return docs


def get_raw_file(path):
    """get raw file"""
    with open(path, "r") as f:
        return f.read()


def get_mongo_doc(path):
    """get mongo doc."""
    with open(path, "r") as f:
        return json.load(f)


class VoxelClientMock():
    """Mongo DB client abstraction interface."""

    def __init__(self):
        self.__data = {}
        super().__init__()

    def load_file(self, collection: str, filepath: str, data) -> None:
        """
        Loads data into voxel mock.

        Args:
            collection (str): Name of collection.
            filepath (str): Key to be stored on filepath.
            data (ANY): data to be stored.
        """
        if collection not in self.__data:
            self.__data[collection] = []
        self.__data[collection].append({"filepath": filepath, "data": data})

    def get_num_entries(self, file_path: str, dataset: VoxelDataset) -> int:
        """
        Return the number of entries in a specific dataset.

        Args:
            file_path (str): Path to the entry.
            dataset (str): Dataset of the entry

        Returns:
            int: Number of entries found
        """
        data = self.__data[dataset.value]
        count = 0
        for entry in data:
            if entry['filepath'] == file_path:
                count += 1
        return count


@pytest.fixture
def db_configuration():
    return NoSQLDBConfiguration("DataIngestion", "dev", None)


@pytest.fixture
def s3_params():
    return S3Params("dev-rcd-anonymized-video-files", "dev-rcd-raw-video-files", "Debug_Lync")


@pytest.fixture(scope="function")
def mock_mongo_client() -> mongomock.MongoClient:
    """mock_mongo_client

    - Drops previously loaded collections to avoid tests side effects.
    - Loads fixtures placed under ``mongo_data`` as collections in mongomock.MongoClient
    - Uses cache mechanism to avoid reading from fixture files on every test.

    Returns:
        mongomock.MongoClient: mongo client
    """
    client = mongomock.MongoClient()
    db = client[MONGO_DB_NAME]
    for name in db.list_collection_names():
        db.drop_collection(name)

    for collection_file in os.listdir(MONGO_DATA):
        full_fixture_path = get_collection_full_fixture_path(collection_file)
        collection, _ = os.path.splitext(os.path.basename(collection_file))
        docs = load_collection_fixture(full_fixture_path)
        db[collection].insert_many(docs)
    return client


@pytest.fixture(scope="session")
def moto_s3_client() -> S3Client:
    """s3_mock.

    Returns:
        Mock: mocked S3 client
    """
    with mock_s3():
        # moto only accepts us-east-1 https://github.com/spulec/moto/issues/3292#issuecomment-718116897
        moto_s3_client = boto3.client("s3", region_name="us-east-1")

        # List and create buckets
        for dir_name in os.listdir(S3_DATA):
            bucket_path = os.path.join(S3_DATA, dir_name)

            if os.path.isfile(bucket_path):
                continue

            moto_s3_client.create_bucket(Bucket=dir_name)

            # List and create files in bucket
            for file_name in os.listdir(bucket_path):
                file_path = os.path.join(bucket_path, file_name)
                moto_s3_client.put_object(
                    Bucket=dir_name,
                    Body=get_raw_file(file_path),
                    Key=os.path.join(
                        "Debug_Lync",
                        file_name))

        yield moto_s3_client


@pytest.fixture
def voxel_client(scope="session") -> VoxelEntriesGetter:
    client = VoxelClientMock()

    # List and create buckets
    for dir_name in os.listdir(VOXEL_DATA):
        dataset_path = os.path.join(VOXEL_DATA, dir_name)

        if os.path.isfile(dataset_path):
            continue

        # List and create files in bucket
        for file_name in os.listdir(dataset_path):
            filepath = f"s3://dev-rcd-anonymized-video-files/Debug_Lync/{file_name}"
            client.load_file(dir_name, filepath, None)

    return client


@pytest.fixture
def db_client(mock_mongo_client, db_configuration) -> MongoDBClient:
    return MongoDBClient(db_configuration, mock_mongo_client)


@pytest.fixture
def document_validator() -> JSONSchemaValidator:
    return JSONSchemaValidator()


@pytest.fixture
def blob_storage_controller(
    s3_params: S3Params,
    moto_s3_client: S3Client
) -> S3Controller:
    return S3Controller(
        s3_params,
        moto_s3_client
    )


@pytest.fixture
def database_controller(
    db_client: MongoDBClient,
    document_validator: JSONSchemaValidator,
) -> DatabaseController:
    return DatabaseController(
        db_client,
        document_validator,
        MagicMock()
    )


@pytest.fixture
def voxel_fiftyone_controller(
    s3_params: S3Params,
    voxel_client: VoxelEntriesGetter
) -> VoxelFiftyOneController:
    return VoxelFiftyOneController(
        s3_params,
        voxel_client
    )
