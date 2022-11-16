"""
Authentication for Metadata API.
"""
import os
from functools import wraps
from typing import Callable

import cognitojwt
from cognitojwt import CognitoJWTException
from flask import abort, request

AWS_COGNITO_REGION = os.environ.get('AWS_COGNITO_REGION')
AWS_COGNITO_USERPOOL_ID = os.environ.get('AWS_COGNITO_USERPOOL_ID')
AWS_COGNITO_APPCLIENT_ID = os.environ.get('AWS_COGNITO_APPCLIENT_ID')
COGNITO_ISS = f"https://cognito-idp.{AWS_COGNITO_REGION}.amazonaws.com/{AWS_COGNITO_USERPOOL_ID}"


class AuthorizationHeaderException(Exception):
    """AuthorizationHeaderException.

    Raised when no authorization header is provided or the header is in an invalid format
    """

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


def _get_token(header_value: str):
    """_get_token.

    Private utility function to extract JWT token from Authorization header value.

    Args:
        header_value (str): authorization header value in format `Bearer <token>`

    Raises:
        AuthorizationHeaderException: error with the authorization header

    Returns:
        str: raw JWT token
    """
    if not header_value:
        raise AuthorizationHeaderException('Authorization header not provided')

    parts = header_value.split('Bearer ')
    if len(parts) < 2 or parts[1] == '':
        raise AuthorizationHeaderException('Authorization header invalid format')

    return parts[1]


def require_auth(api_method: Callable):
    """require_auth.

    Decorator function that provides JWT authentication to a wrapped flask endpoint.

    Args:
        api_method (Callable): endpoint function to be wrapped.

    Returns:
        Callable: wrapped function
    """
    @wraps(api_method)
    def check_auth_header(*args, **kwargs):
        authorization_header = request.headers.get('Authorization')
        if not authorization_header:
            abort(401)

        try:
            access_token = _get_token(authorization_header)
            claims: dict = cognitojwt.decode(
                token=access_token,
                region=AWS_COGNITO_REGION,
                userpool_id=AWS_COGNITO_USERPOOL_ID,
                # This will check `aud` claim
                app_client_id=AWS_COGNITO_APPCLIENT_ID)

            if claims['iss'] != COGNITO_ISS:
                abort(401)
            return api_method(*args, **kwargs)
        except (CognitoJWTException, AuthorizationHeaderException):
            return abort(401)

    return check_auth_header
