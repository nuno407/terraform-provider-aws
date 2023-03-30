""" message persistence module. """
import json
import logging
from logging import Logger

from kink import inject
from pymongo import MongoClient

from base.aws.model import SQSMessage
from sanitizer.config import SanitizerConfig

_logger: Logger = logging.getLogger(__name__)


@inject
class MessagePersistence:  # pylint: disable=too-few-public-methods
    """ Message persistence class. """

    def __init__(self,
                 mongo_client: MongoClient,
                 config: SanitizerConfig) -> None:
        self.mongo_client = mongo_client
        self.config = config

    def save(self, message: SQSMessage) -> None:
        """ Saves given message in database collection """
        collection = self.mongo_client[self.config.db_name][self.config.message_collection]
        _logger.info("Saving message in collection %s", self.config.message_collection)
        document = self.__to_dict(message)
        collection.insert_one(document)

    def __to_dict(self, message: SQSMessage) -> dict:
        """ returns dictionary from dataobject

        Returns:
            dict: dictionary for given dataobject
        """
        return json.loads(json.dumps(message, default=lambda o: o.__dict__))
