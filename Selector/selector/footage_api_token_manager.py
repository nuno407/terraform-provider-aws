""" Token Management for RCC Footage API. """
import base64
import json
import logging
from datetime import datetime
from typing import Optional
from urllib.parse import urlencode

import urllib3

_logger = logging.getLogger(__name__)


class InvalidConfiguration(Exception):
    """ Exception for missing credentials. """


class FootageApiTokenManager():  # pylint: disable=too-few-public-methods,too-many-instance-attributes
    """ Contains all the operations related with the Token Management for the RCC Footage API. """

    def __init__(self, token_endpoint: str, client_id: str, client_secret: str):

        if not token_endpoint:
            raise InvalidConfiguration("Footage API Token Endpoint is missing.")
        self.__token_endpoint = token_endpoint

        if not client_id or not client_secret:
            raise InvalidConfiguration("client_id or client_secret is empty.")
        self.__client_id = client_id
        self.__client_secret = client_secret

        self.__access_token: Optional[str] = None
        self.__token_expires = int(datetime.now().timestamp())
        self.__http_client = urllib3.PoolManager()

    def get_token(self):
        """ Obtain Footage API Token. """
        current_timestamp_s = int(datetime.now().timestamp())
        if self.__access_token and self.__token_expires > current_timestamp_s:
            _logger.info("Using cached token which expires at %s", self.__token_expires)
            return self.__access_token

        token = self.__request_token()
        if token:
            # Substract 5 minutes from the expiration date to avoid
            # expired tokens due to processing time, network delay, etc.
            # 5 minutes is a random chosen value.
            self.__token_expires = current_timestamp_s + token.get("expires_in") - (5 * 60)
            self.__access_token = token.get("access_token")
            return self.__access_token

        # Warning has already been logged
        return None

    def set_token(self, access_token: str, token_expires: int):
        """ Externaly set of token. Used for testing. """
        self.__access_token = access_token
        self.__token_expires = token_expires

    def __request_token(self):
        client_auth = base64.b64encode((self.__client_id + ":" + self.__client_secret).encode("utf-8")).decode("utf-8")

        headers = {}
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        headers["Authorization"] = "Basic " + client_auth

        body = {
            "grant_type": "client_credentials"
        }
        encoded_body = urlencode(body)

        try:
            _logger.debug(
                "Auth token request: Endpoint: %s, Headers: %s, Body: %s",
                self.__token_endpoint,
                headers,
                body)
            response = self.__http_client.request("POST", self.__token_endpoint, headers=headers, body=encoded_body)

            if response.status == 200:
                json_response = json.loads(response.data.decode("utf-8"))
                _logger.info("Successfully requested auth token")
                return json_response

            _logger.warning("Error getting access token, status: %s, cause: %s", response.status, response.data)
            return None
        except json.JSONDecodeError:
            _logger.warning("String could not be converted to JSON")
            return None
