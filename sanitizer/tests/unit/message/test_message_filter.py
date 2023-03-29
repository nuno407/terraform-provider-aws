from datetime import datetime
from unittest.mock import Mock, PropertyMock

import pytest

from base.aws.model import MessageAttributes, SQSMessage
from sanitizer.config import SanitizerConfig
from sanitizer.message.message_filter import MessageFilter


@pytest.mark.unit
class TestMessageFilter:

    def _message(self, tenant_id: str) -> SQSMessage:
        return SQSMessage(
            message_id="foo",
            receipt_handle="bar",
            timestamp=datetime.now(),
            body="{}",
            attributes=MessageAttributes(tenant=tenant_id, device_id="baz"),
        )

    def _config(self) -> SanitizerConfig:
        config_mock = Mock()
        config_mock.tenant_blacklist = ["foo"]
        return config_mock

    def test_blacklisted_message_is_filtered(self):
        message = self._message("foo")
        config = self._config()
        assert not MessageFilter(config).is_relevant(message)

    def test_non_blacklisted_message_is_not_filtered(self):
        message = self._message("bar")
        config = self._config()
        assert MessageFilter(config).is_relevant(message)

    def test_message_with_no_tenant_is_not_filtered(self):
        message = self._message(None)
        config = self._config()
        assert MessageFilter(config).is_relevant(message)
