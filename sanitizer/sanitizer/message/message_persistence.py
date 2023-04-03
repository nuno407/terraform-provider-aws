""" message persistence module. """
import dataclasses
import logging
from datetime import datetime
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
        document = self.to_persistable_dict(message)
        collection.insert_one(document)

    def to_persistable_dict(self, message: SQSMessage) -> dict:
        """ returns timeseries persistable dictionary from dataobject

        Returns:
            dict: dictionary for given dataobject
        """
        return {
            "message_id": message.message_id,
            "receipt_handle": message.receipt_handle,
            "timestamp": DateUtils.from_iso8601_to_datetime(message.timestamp),
            "body": message.body,
            "attributes": dataclasses.asdict(message.attributes)
        }


class DateUtils:  # pylint: disable=too-few-public-methods
    """ Date utils class. """
    @staticmethod
    def from_iso8601_to_datetime(timestamp: str) -> datetime:
        """ Converts from ISO8601 to datetime """
        iso8601_accepted_datetime = timestamp.replace("Z", "+00:00")
        return datetime.fromisoformat(iso8601_accepted_datetime)
