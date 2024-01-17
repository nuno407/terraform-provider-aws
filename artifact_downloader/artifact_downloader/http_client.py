""" HTTP client for sending requests against Metadata artifact API """

import logging

import backoff
from backoff.types import Details
from requests import Request, Response, Session
from kink import inject

from artifact_downloader.exceptions import (RetriableFailureReturnCode,
                                            UnexpectedReturnCode)

_logger = logging.getLogger(__name__)

MAX_TRIES = 3


def _on_backoff_handler(details: Details):
    """ Handler for backoff on IVS. """
    _logger.info("Backing off %.1f seconds after %s tries",
                 details.get("wait"), details.get("tries"))


def _on_success_handler(details: Details):
    """ Handler for success of ivs API"""
    elapsed_val = details["elapsed"]
    elapsed = f"{elapsed_val:0.1f}"
    _logger.info("HTTP request sent successfully after %s seconds",
                 elapsed)


@inject
class HttpClient:   # pylint: disable=too-few-public-methods
    """ HTTP client for sending requests against Metadata artifact API """

    def __init__(self, session: Session):
        self.__session = session

    @backoff.on_exception(backoff.expo,
                          RetriableFailureReturnCode,
                          max_tries=MAX_TRIES,
                          on_backoff=_on_backoff_handler,
                          on_success=_on_success_handler)
    def execute_request(self, req: Request) -> Response:
        """ Executes a request, checks with the allowed return codes and retries if neccessary. """

        _logger.info("Sending request to %s", req.url)
        resp = self.__session.send(req.prepare())
        if resp.status_code != 200:
            raise UnexpectedReturnCode(
                f"Received {resp.status_code} status code when sending request {resp.request}: {resp.text}")
        return resp
