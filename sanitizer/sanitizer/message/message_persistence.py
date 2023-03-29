""" message persistence module. """
import logging
from logging import Logger

from kink import inject
from pymongo import MongoClient

from base.aws.model import SQSMessage
from sanitizer.config import SanitizerConfig

_logger: Logger = logging.getLogger(__name__)

@inject
class MessagePersistence: # pylint: disable=too-few-public-methods
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
        collection.insert_one(message.stringify())
