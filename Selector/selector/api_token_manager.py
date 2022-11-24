""" Token Management for RCC Footage API. """
import base64
from datetime import datetime
import json
import logging
from urllib.parse import urlencode
import boto3
import urllib3

_logger = logging.getLogger(__name__)


class ApiTokenManager():  # pylint: disable=too-few-public-methods
    """ Contains all the operations related with the Token Management for the RCC Footage API. """

    def __init__(self, token_endpoint, secret_id):
        self.__token_endpoint = token_endpoint
        secret_manager_client = boto3.client("secretsmanager", region_name="eu-central-1")
        get_secret_response = secret_manager_client.get_secret_value(SecretId=secret_id)
        self.__secret = json.loads(get_secret_response["SecretString"])
        self.__access_token = None
        self.__token_expires = int(datetime.now().timestamp())
        self.__http_client = urllib3.PoolManager()

    def get_token(self):
        """ Obtain Footage API Token. """
        current_timestamp_s = int(datetime.now().timestamp())
        if self.__access_token and self.__token_expires > current_timestamp_s:
            _logger.info("using cached token which expires at %s", self.__token_expires)
            return self.__access_token

        if self.__secret:
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

        _logger.error("Not getting token because the secret is empty.")
        return None

    def __request_token(self):
        client_id = self.__secret["client_id"]
        client_secret = self.__secret["client_secret"]
        client_auth = base64.b64encode((client_id + ":" + client_secret).encode("utf-8")).decode("utf-8")

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
