"""
Authentication for Metadata API.
"""
import os
from functools import wraps
from typing import Any, Callable
import logging
import requests
from flask import abort, request
from jose import jwt
from jose.exceptions import (JWTError, ExpiredSignatureError, JWTClaimsError)
from requests import Timeout
from requests import JSONDecodeError

AZURE_CLIENT_ID = os.environ.get("AZURE_CLIENT_ID")
AZURE_TENANT_ID = os.environ.get("AZURE_TENANT_ID")
JWKS_REQ_TIMEOUT = os.environ.get("REQUEST_TIMEOUT", 1000)
REQUIRED_SCOPES = os.environ.get("REQUIRED_SCOPES", "access")
AZURE_OIDC_BASE_URL = os.environ.get("AZURE_OIDC_BASE_URL", "https://login.microsoftonline.com")
AZURE_OIDC_URL = f"{AZURE_OIDC_BASE_URL}/{AZURE_TENANT_ID}"
AZURE_OIDC_JWKS_URL = f"{AZURE_OIDC_URL}/discovery/v2.0/keys"
AZURE_ISS_URL = f"{AZURE_OIDC_BASE_URL}/{AZURE_TENANT_ID}/v2.0"

_logger: logging.Logger = logging.getLogger(__name__)

class AuthenticationError(Exception):
    """AuthenticationError.

    Raised when something went wrong in the access token authentication flow.
    """

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


def _get_token_from_header(header_value: str):
    """_get_token_from_header.

    Private utility function to extract JWT token from Authorization header value.

    Args:
        header_value (str): authorization header value in format `Bearer <token>`

    Raises:
        AuthenticationError: error with the authorization header

    Returns:
        str: raw JWT token
    """
    parts = header_value.split("Bearer ")
    if len(parts) < 2 or parts[1] == "":
        raise AuthenticationError("Authorization header invalid format")

    return parts[1]


def _get_rsa_key(access_token: str) -> dict[str, Any]:
    """_get_rsa_key

    Args:
        access_token (str): encoded access token to be parsed.

    Raises:
        JSONDecodeError: error parsing the jwks endpoint response
        Timeout: error on HTTP GET timeout to the jwks url
        AuthenticationError: error when kid is not found in the JWKS response

    Returns:
        dict[str, Any]: the JWKS pubkey for the kid in given token header
    """
    unverified_token_header = jwt.get_unverified_header(access_token)
    jwks_response: requests.Response = requests.get(AZURE_OIDC_JWKS_URL, timeout=int(JWKS_REQ_TIMEOUT))
    jwks = jwks_response.json()
    pub_keys = [key for key in jwks["keys"] if key["kid"] == unverified_token_header["kid"]]
    if len(pub_keys):
        key = pub_keys[0]
        return {
            "kty": key["kty"],
            "kid": key["kid"],
            "use": key["use"],
            "n": key["n"],
            "e": key["e"],
        }
    raise AuthenticationError("kid not found in the JWKS URL")


def require_auth(api_method: Callable):
    """require_auth.

    Decorator function that provides JWT authentication to a wrapped flask endpoint.

    Args:
        api_method (Callable): endpoint function to be wrapped.

    Returns:
        Callable: wrapped function
    """
    @ wraps(api_method)
    def check_auth_header(*args, **kwargs):
        authorization_header = request.headers.get("Authorization")
        if not authorization_header:
            abort(401)

        try:
            access_token = _get_token_from_header(authorization_header)
            rsa_key = _get_rsa_key(access_token)
            token_claims: dict = jwt.decode(
                access_token,
                rsa_key,
                algorithms=["RS256"],
                audience=AZURE_CLIENT_ID,
                issuer=AZURE_ISS_URL
            )
            if any([scp not in token_claims["scope"] for scp in REQUIRED_SCOPES.split(' ')]):
                abort(401)

            return api_method(*args, **kwargs)
        except Timeout:
            _logger.error("Timeout reaching JWKS endpoint")
            return abort(401)
        except JSONDecodeError:
            _logger.error("Error parsing JWKS response")
            return abort(401)
        except (AuthenticationError, JWTError, ExpiredSignatureError, JWTClaimsError) as err:
            _logger.error("Authentication error: %s", err)
            return abort(401)

    return check_auth_header
