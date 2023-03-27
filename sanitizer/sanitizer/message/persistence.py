""" message persistence module. """
from pymongo import MongoClient

from base.aws.model import SQSMessage

class MessagePersistence:
    """ Message persistence class. """

    def __init__(self, mongo_client: MongoClient) -> None:
        self.mongo_client = mongo_client

    def save(self, message: SQSMessage) -> None:
        """ Saves given message in database collection """
        raise NotImplementedError("TODO")
