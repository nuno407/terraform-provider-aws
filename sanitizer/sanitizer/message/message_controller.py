""" message wrapper module. """

from kink import inject
from sanitizer.message.message_parser import MessageParser
from sanitizer.message.message_filter import MessageFilter
from sanitizer.message.message_persistence import MessagePersistence


@inject
class MessageController:  # pylint: disable=too-few-public-methods
    """ Message wrapper class. """

    def __init__(self,
                 parser: MessageParser,
                 mfilter: MessageFilter,
                 persistence: MessagePersistence) -> None:
        self.parser = parser
        self.filter = mfilter
        self.persistence = persistence
