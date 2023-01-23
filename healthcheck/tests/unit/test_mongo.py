from unittest.mock import MagicMock, Mock, patch

import pytest

from healthcheck.mongo import (DBCollection, DBDocument, MongoDBClient,
                               NoSQLDBConfiguration)


@pytest.mark.unit
class TestMongoDBClient:

    @pytest.fixture
    def fix_db_configuration(self):
        return NoSQLDBConfiguration(
            db_name="test-db",
            environment_prefix="test",
            db_uri="unit"
        )

    @pytest.fixture
    def fix_client(self, fix_db_configuration: NoSQLDBConfiguration):
        return MongoDBClient(
            fix_db_configuration,
            MagicMock()
        )

    def test_find_one(self, fix_client: MongoDBClient):
        mock_collections = MagicMock()
        collection = Mock()
        collection.find_one = Mock(
            return_value=DBDocument({"test": "document"}))
        mock_collections.__getitem__ = Mock(return_value=collection)
        with patch("healthcheck.mongo.MongoDBClient.collections", mock_collections):
            db_doc = fix_client.find_one(
                DBCollection.RECORDINGS, id_field="field1", id_value="value1")
            assert isinstance(db_doc, dict)
            collection.find_one.assert_called_once_with({"field1": "value1"})

    def test_get_collection_name(self, fix_client: MongoDBClient):
        assert "test-recordings" == fix_client.get_collection_name(
            DBCollection.RECORDINGS)
