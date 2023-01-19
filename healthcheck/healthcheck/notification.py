import json
import urllib.request
from logging import Logger
from typing import Protocol, Optional

from kink import inject


class Notifier(Protocol):
    def send_notification(self, message: str) -> None:
        ...

@inject(alias=Notifier)
class MSTeamsWebhookNotifier:
    def __init__(self, webhook_url: Optional[str], logger: Logger):
        self.__webhook_url = webhook_url
        self.__logger = logger

    def send_notification(self, message: str) -> None:
        """Sends MSTeams MessageCard notification via webhook

        Args:
            message (str): message to be displayed

        Raises:
            ValueError: if invalid url is provided
        """
        if not self.__webhook_url:
            self.__logger.info("MSTEAMS_WEBHOOK not set, unable to notify")

        if not self.__webhook_url.lower().startswith('http'):
            raise ValueError("Invalid URL")

        body = MSTeamsWebhookNotifier.get_body(message)
        request = urllib.request.Request(self.__webhook_url, method='POST')
        request.add_header('Content-Type', 'application/json; charset=utf-8')
        jsondata = json.dumps(body)
        content_bytes = jsondata.encode('utf-8')
        request.add_header('Content-Length', len(content_bytes))
        with urllib.request.urlopen(request, content_bytes) as response: # nosec
            self.__logger.info("webhook notifier response: %s", response.status)

    @staticmethod
    def get_body(message: str) -> dict:
        return {
            "@context": "https://schema.org/extensions",
            "@type": "MessageCard",
            "themeColor": "0072C6",
            "title": "Healthcheck Alert",
            "text": message
        }
