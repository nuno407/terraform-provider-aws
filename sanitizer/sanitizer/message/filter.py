from typing import Optional

from base.aws.model import SQSMessage
from sanitizer.config import SanitizerConfig


class MessageFilter:
    def __init__(self, config: SanitizerConfig) -> None:
        self.__config = config

    def is_relevant(self, message: SQSMessage) -> bool:
        """Check if message is relevant.

        Args:
            message (SQSMessage): parsed SQS message

        Returns:
            bool: true if message is relevant
        """

        return all([
            not self.__is_tenant_blacklisted(message)
            # add more filters here
        ])

    def __is_tenant_blacklisted(self, message: SQSMessage) -> bool:
        """Check if message is from blacklisted tenant.

        Args:
            message (SQSMessage): parsed SQS message

        Returns:
            bool: verification result
        """
        if message.attributes.tenant is None:
            return False
        else:
            return message.attributes.tenant in self.__config.tenant_blacklist
