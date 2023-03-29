""" Message persistence module tests. """
import json
from unittest.mock import MagicMock

import pytest

from sanitizer.message.message_persistence import MessagePersistence


@pytest.mark.unit
def test_save():
    """ Test save method. """
    mongo_client = MagicMock()
    config = MagicMock()
    message = MagicMock()

    persistence = MessagePersistence(mongo_client, config)
    persistence.save(message)

    collection = mongo_client[config.db_name][config.message_collection]
    collection.insert_one.assert_called_once_with(json.loads(json.dumps(message, default=lambda o: o.__dict__)))
