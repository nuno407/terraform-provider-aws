from typing import Optional

from sanitizer.model import SQSMessage


class MessageFilter:
    def apply(self, message: SQSMessage) -> Optional[SQSMessage]:
        raise NotImplementedError("TODO")
