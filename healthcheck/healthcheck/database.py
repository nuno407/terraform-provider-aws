"""Database interface module."""
from dataclasses import dataclass
from enum import Enum
from typing import Protocol

from healthcheck.model import DBDocument


class DBCollection(Enum):
    """DB collections available."""
    RECORDINGS = "recordings"
    PIPELINE_EXECUTION = "pipeline-execution"
    SIGNALS = "signals"
    ALGORITHM_OUTPUT = "algorithm-output"


class INoSQLDBClient(Protocol):
    """NoSQL DB client abstraction interface."""

    def find_many(self, collection: DBCollection, query: dict) -> list[DBDocument]:
        """find many documents in a collection based on the query

        Args:
            collection (DBCollection): database collection
            query (dict): database query

        Returns:
            list[DBDocument]: result list of documents
        """

    def aggregate(self, collection: DBCollection, pipelines: list[dict]) -> list[DBDocument]:
        """query aggregation documents based on base collection and pipelines

        Args:
            collection (DBCollection): base aggregation collection
            pipelines (list[dict]): database aggregation pipelines

        Returns:
            list[DBDocument]: result list of documents
        """

    def get_collection_name(self, collection: DBCollection) -> str:
        """get collection name

        Args:
            collection (DBCollection): collection enum

        Returns:
            str: DB collection name
        """


@dataclass
class NoSQLDBConfiguration():
    """DB configuration."""
    db_name: str
    environment_prefix: str
    db_uri: str
