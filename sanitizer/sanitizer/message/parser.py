from kink import inject

from sanitizer.message.persistence import MessagePersistence
from sanitizer.model import SQSMessage


@inject
class MessageParser:
    def __init__(self, message_persistence: MessagePersistence) -> None:
        self.message_persistence = message_persistence

    def parse(self, raw_message: str) -> SQSMessage:
        raise NotImplementedError("TODO")
