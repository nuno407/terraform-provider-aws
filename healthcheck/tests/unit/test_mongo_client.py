"""test mongo module"""
import pytest
from healthcheck.mongo import MongoDBClient
from healthcheck.database import DBConfiguration, DBCollection
from unittest.mock import MagicMock

@pytest.mark.unit
class TestMongoDBClient():

    @pytest.fixture
    def db_config(self) -> DBConfiguration:
        return DBConfiguration("test-db", "test", "test-uri")

    @pytest.fixture
    def client_mock(self) -> MagicMock:
        return MagicMock()

    @pytest.fixture
    def mongo_db_client(self, db_config: DBConfiguration, client_mock: MagicMock) -> MongoDBClient:
        return MongoDBClient(db_config, client_mock)

    def test_setup(self, mongo_db_client: MongoDBClient):
        for collection_enum in DBCollection:
            assert collection_enum in mongo_db_client.collections.keys()
