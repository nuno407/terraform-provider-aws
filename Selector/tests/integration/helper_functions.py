"Helper functions for integration tests"
import os
from bson import json_util
from pymongo import MongoClient
from base.testing.utils import load_relative_json_file, load_relative_raw_file, load_relative_str_file


def get_sqs_message(file_name: str) -> dict:
    """
    Retrieve a json file from the data folder.

    Args:
        file_name (str): The file name

    Returns:
        dict: The json file
    """
    return load_relative_json_file(__file__, os.path.join("data", file_name))


def get_s3_file(file_name: str) -> bytes:
    """
    Retrieve a file from the data folder.

    Args:
        file_name (str): The file name

    Returns:
        bytes: The file
    """
    return load_relative_raw_file(__file__, os.path.join("data", file_name))


def load_database(mongo_client: MongoClient, database: str, collection: str, file_name: str):
    """
    Load a database with a specific collection and file.
    The format needs to be a mongo json in the extended format.
    (More info here: https://www.mongodb.com/docs/drivers/java/sync/current/fundamentals/data-formats/document-data-format-extended-json/) # pylint: disable=line-too-long

    Example on how to dump a collection:
    -"mongosh <mongo_con_string> --quiet --eval "JSON.stringify(EJSON.serialize(db['dev-recordings'].find().toArray()))"
    Args:
        mongo_client (MongoClient): The mongo client
        database (str): The database name
        collection (str): The collection name
        file_name (str): The file name
    """
    raw_dump = load_relative_str_file(__file__, os.path.join("data","mongo_dumps", file_name))
    mongo_client[database][collection].insert_many(json_util.loads(raw_dump))
