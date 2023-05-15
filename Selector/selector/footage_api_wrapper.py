""" RCC Footage API Wrapper. """
import json
import logging

import urllib3
from kink import inject
from urllib3 import Retry

from selector.footage_api_token_manager import FootageApiTokenManager

_logger = logging.getLogger(__name__)


@inject
class FootageApiWrapper:  # pylint: disable=too-few-public-methods
    """ Contains all the operations related with the RCC Footage API. """

    def __init__(self,
                 footage_api_url: str,
                 footage_api_token_manager: FootageApiTokenManager):
        retries = Retry(total=3, backoff_factor=1, allowed_methods=["POST"], status_forcelist=[500])
        self.__http_client = urllib3.PoolManager(retries=retries)
        self.__footage_api_url = footage_api_url
        self.__footage_api_token_manager = footage_api_token_manager

    def request_recorder(self, recorder: str, device_id: str, from_timestamp: int, to_timestamp: int):
        """Request the upload of a given recorder from a device between two timestamps

        Args:
            recorder (str): the recorder to request
            device_id (str): device identifier
            from_timestamp (int): starting epoch timestamp (UTC), millisecond precision
            to_timestamp (int): ending epoch timestamp (UTC), millisecond precision

        Raises:
            RuntimeError: _description_
        """
        auth_token = self.__footage_api_token_manager.get_token()
        if not auth_token:
            _logger.error("Could not get auth token for Footage API. Skipping request.")

        payload = {"from": str(from_timestamp), "to": str(to_timestamp), "recorder": recorder}
        url = self.__footage_api_url.format(device_id)

        headers = {"Content-Type": "application/json", "Authorization": "Bearer " + auth_token}
        body = json.dumps(payload)

        _logger.info(
            "Requesting upload of %s on device %s from %i to %i",
            recorder,
            device_id,
            from_timestamp,
            to_timestamp)
        response = self.__http_client.request("POST", url, headers=headers, body=body)

        if (response.status >= 200 and response.status < 300):
            _logger.info("Successfully requested footage with response code %i", response.status)
        else:
            _logger.warning("Unexpected response when requesting footage: %i*", response.status)
            if response.content:
                _logger.warning("Details: %s", response.content)
            raise RuntimeError  # SonarQube doesn't accept "Exception", it needs a more specific one
