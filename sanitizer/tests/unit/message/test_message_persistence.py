""" Message persistence module tests. """
from unittest.mock import MagicMock

import pytest

from sanitizer.message.message_persistence import MessagePersistence


@pytest.mark.unit
def test_save():
    """ Test save method. """
    mongo_client = MagicMock()
    config = MagicMock()
    message = MagicMock()
    message.stringify.return_value = "stringified message"

    persistence = MessagePersistence(mongo_client, config)
    persistence.save(message)

    message.stringify.assert_called_once()
    collection = mongo_client[config.db_name][config.message_collection]
    collection.insert_one.assert_called_once_with("stringified message")
