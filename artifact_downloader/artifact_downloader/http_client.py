""" HTTP client for sending requests against Metadata artifact API """

import logging

import backoff
from backoff.types import Details
from requests import Request, Response, Session

from artifact_downloader.exceptions import (RetriableFailureReturnCode,
                                            UnexpectedReturnCode)

__logging = logging.getLogger(__name__)

MAX_TRIES = 3


def _on_backoff_handler(details: Details):
    """ Handler for backoff on IVS. """
    __logging.info("Backing off %.1f seconds after %s tries",
                   details.get("wait"), details.get("tries"))


def _on_success_handler(details: Details):
    """ Handler for success of ivs API"""
    elapsed = f"{details['elapsed']:0.1f}"
    __logging.info("HTTP request sent successfully after %s seconds",
                   elapsed)


class HttpClient:   # pylint: disable=too-few-public-methods
    """ HTTP client for sending requests against Metadata artifact API """

    @backoff.on_exception(backoff.expo,
                          RetriableFailureReturnCode,
                          max_tries=MAX_TRIES,
                          on_backoff=_on_backoff_handler,
                          on_success=_on_success_handler)
    def execute_request(self, req: Request, allowed_return_codes: list[int]) -> Response:
        """ Executes a request, checks with the allowed return codes and retries if neccessary. """
        session = Session()
        resp = session.send(req.prepare())
        if resp.status_code not in allowed_return_codes:
            if resp.status_code >= 500:
                raise RetriableFailureReturnCode(
                    f"Received {resp.status_code} status code when executing request. Will retry.")
            raise UnexpectedReturnCode(
                f"Received {resp.status_code} status code when sending request {resp.request}: {resp.text}")
        return resp
