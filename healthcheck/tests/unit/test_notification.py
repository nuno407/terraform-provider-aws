import json
import urllib.request
from unittest.mock import ANY, MagicMock, Mock, patch

import pytest

from healthcheck.notification import MSTeamsWebhookNotifier


@pytest.mark.unit
class TestNotifier:
    @pytest.mark.parametrize("message", [
        ("message1"),
        ("a loonger loooonger looooooooonger looooooooonoooooger message 2")
    ])
    @patch("healthcheck.notification.urllib.request.urlopen")
    def test_send_notification(self, mock_request: Mock, message: str):
        mocked_url = "http://mymockedurl"
        notifier = MSTeamsWebhookNotifier(mocked_url, MagicMock())
        notifier.send_notification(message)

        request = urllib.request.Request(mocked_url)
        request.add_header('Content-Type', 'application/json; charset=utf-8')

        expect_body = MSTeamsWebhookNotifier.get_body(message)
        jsondata = json.dumps(expect_body)
        content_bytes = jsondata.encode('utf-8')
        request.add_header('Content-Length', len(content_bytes))

        mock_request.assert_called_once_with(ANY, content_bytes)
