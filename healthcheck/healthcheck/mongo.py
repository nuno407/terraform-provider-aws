"""MongoDBClient module."""
import logging

from kink import inject
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

from healthcheck.model import DBDocument
from healthcheck.database import INoSQLDBClient, DBCollection, NoSQLDBConfiguration

_logger: logging.Logger = logging.getLogger(__name__)


@inject(alias=INoSQLDBClient)
class MongoDBClient():
    """MongoDB Metadata Data Access."""

    def __init__(self, db_config: NoSQLDBConfiguration, client: MongoClient) -> None:
        """initalizes mongodb data access object

        Args:
            db_config (DBConfiguration): database configuration wrapper
            client (MongoClient): pymongo client
        """
        self.db_config = db_config
        self.client = client
        self.database: Database = self.client[db_config.db_name]
        self.__collections: dict[DBCollection, Collection] = {}
        self._setup_collections()

    @property
    def collections(self) -> dict[DBCollection, Collection]:
        """current mongodb collections

        Returns:
            dict[DBCollection, Collection]: all existing collections
        """
        return self.__collections

    def _setup_collections(self) -> None:
        """Initialize collections"""
        for collection in DBCollection:
            env_collection_name = f"{self.db_config.environment_prefix}-{collection.value}"
            _logger.info("setting up collection %s", env_collection_name)
            self.__collections[collection] = self.database[env_collection_name]

    def find_one(self, collection: DBCollection, id_field: str, id_value: str) -> DBDocument:
        """find_one.

        Queries one document by id

        Args:
            collection (DBCollection): mongodb collection
            id_field (str): identifier field
            id_value (str): identifier value

        Returns:
            DBDocument: result document
        """
        query: dict = {}
        query[id_field] = id_value
        result = self.collections[collection].find_one(query)
        return DBDocument(result)

    def find_many(self, collection: DBCollection, query: dict) -> list[DBDocument]:
        """find_many.

        Query many documents.

        Args:
            collection (DBCollection): mongodb collection
            query (dict): pymongo query object

        Returns:
            list[DBDocument]: result documents
        """
        result_set = self.collections[collection].find(query)
        return [DBDocument(result) for result in result_set]

    def aggregate(self, collection: DBCollection, pipelines: list[dict]) -> list[DBDocument]:
        """aggregate.

        Query aggregated documents in mongo.

        Args:
            collection (DBCollection): mongodb collection
            pipelines (list[dict]): mongo aggregation pipelines

        Returns:
            list[DBDocument]: result documents
        """
        return self.collections[collection].aggregate(pipelines)

    def get_collection_name(self, collection: DBCollection) -> str:
        """get_collection_name.

        Query collection name by environment

        Args:
            collection (DBCollection): DB collection

        Returns:
            str: full collection name
        """
        return f"{self.db_config.environment_prefix}-{collection.value}"
