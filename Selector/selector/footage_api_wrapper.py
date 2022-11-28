""" RCC Footage API Wrapper. """
import json
import logging
import urllib3
from urllib3 import Retry
from selector.footage_api_token_manager import FootageApiTokenManager

_logger = logging.getLogger(__name__)


class FootageApiWrapper():  # pylint: disable=too-few-public-methods
    """ Contains all the operations related with the RCC Footage API. """

    def __init__(self, footage_api_url: str, footage_api_token_manager: FootageApiTokenManager):

        retries = Retry(total=3, backoff_factor=1, allowed_methods=["POST"], status_forcelist=[500])
        self.__http_client = urllib3.PoolManager(retries=retries)

        self.__footage_api_url = footage_api_url

        self.__footage_api_token_manager = footage_api_token_manager

    def request_footage(self, device_id: int, from_timestamp: int, to_timestamp: int):
        """ Requests footage upload by RCC. """
        auth_token = self.__footage_api_token_manager.get_token()
        if not auth_token:
            _logger.error("Could not get auth token for Footage API. Skipping request.")
            return

        payload = {"from": str(from_timestamp), "to": str(to_timestamp), "recorder": "TRAINING"}
        url = self.__footage_api_url.format(device_id)

        headers = {}
        headers["Content-Type"] = "application/json"
        headers["Authorization"] = "Bearer " + auth_token
        body = json.dumps(payload)

        try:
            _logger.info(
                "Requesting footage upload from url '%s' with timestamp from %i to %i",
                url,
                from_timestamp,
                to_timestamp)
            response = self.__http_client.request("POST", url, headers=headers, body=body)

            if (response.status >= 200 and response.status < 300):
                _logger.info("Successfully requested footage with response code %i", response.status)
            else:
                _logger.warning("Unexpected response when requesting footage: %i*", response.status)
                if response.content:
                    _logger.warning("Details: %s", response.content)
        except Exception as error:  # pylint: disable=broad-except
            _logger.error("Unexpected error occured when requesting footage: %s", error)
