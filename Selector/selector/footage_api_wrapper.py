""" RCC Footage API Wrapper. """
import json
import logging
from datetime import datetime

import urllib3
from kink import inject
from urllib3 import Retry

from base.model.artifacts import RecorderType

from selector.footage_api_token_manager import FootageApiTokenManager
from selector.constants import FOOTAGE_RECORDER_NAME_MAP
from selector.exceptions import RecorderNotImplemented

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

    @staticmethod
    def __convert_input_recorder_type(recorder_type: RecorderType) -> str:
        if recorder_type not in FOOTAGE_RECORDER_NAME_MAP:
            raise RecorderNotImplemented(
                f"The requested recorder type ({recorder_type.value}) \
                does not have an entry in FOOTAGE_RECORDER_NAME_MAP")

        return FOOTAGE_RECORDER_NAME_MAP[recorder_type]

    def request_recorder(self, recorder_type: RecorderType, device_id: str,
                         from_datetime: datetime, to_datetime: datetime):
        """Request the upload of a given recorder from a device between two timestamps

        Args:
            recorder (RecorderType): the recorder to request
            device_id (str): device identifier
            from_datetime (int): starting datetime (UTC)
            to_datetime (int): ending datetime (UTC)

        Raises:
            RuntimeError: _description_
        """
        _logger.info("Requesting %s footage between %s and %s", recorder_type.value, str(from_datetime),
                     str(to_datetime))

        recorder = self.__convert_input_recorder_type(recorder_type)
        from_timestamp = int(from_datetime.timestamp() * 1000)
        to_timestamp = int(to_datetime.timestamp() * 1000)

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
