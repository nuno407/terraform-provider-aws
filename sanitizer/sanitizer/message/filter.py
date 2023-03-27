from typing import Optional

from base.aws.model import SQSMessage


class MessageFilter:
    def apply(self, message: SQSMessage) -> Optional[SQSMessage]:
        raise NotImplementedError("TODO")
