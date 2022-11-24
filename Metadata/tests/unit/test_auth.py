# pylint: disable=redefined-outer-name
"""Test for API authentication module."""

from unittest.mock import Mock

import requests
import metadata.api.auth
import pytest
from flask import Flask
from flask.testing import FlaskClient
from jose.exceptions import JWTError
from metadata.api.auth import AZURE_OIDC_BASE_URL
from pytest_mock import MockerFixture
from requests import JSONDecodeError

TEST_ENDPOINT = "/test"
TEST_KID = "nOo3ZDrODXEK1jKWhZYZ"
TEST_TENANT_ID = "0ae51e19-07c8-4e4b-bb6d-12345"
TEST_AUTHORIZATION_HEADER = "Bearer dGVzdHRva2VuaGVhZGVyCg.dGVzdHRva2VuYm9keQo.dGVzdHRva2Vuc2lnbmF0dXJlCg"
TEST_JWKS = {"keys": [{"kty": "RSA",
                       "use": "sig",
                       "kid": TEST_KID,
                       "x5t": "nOo3ZDrODXEK1jKWhXslHR_KXEg",
                       "n": "oaLLT9hkcSj",
                       "e": "AQAB",
                       "x5c": ["MIIDBTCCAe2gAwIBAgIQN33ROaIJ6b"],
                       "issuer":f"{AZURE_OIDC_BASE_URL}/{TEST_TENANT_ID}/v2.0"},
                      {"kty": "RSA",
                       "use": "sig",
                       "kid": "l3sQ-50cCH4xBVZLHYXZ",
                       "x5t": "l3sQ-50cCH4xBVZLHTGwnSR7680",
                       "n": "sfsXMXWuO-d",
                       "e": "AQAB",
                       "x5c": ["MIIDBTCCAe2gAwIBAgIQWPB1ofOpA7"],
                       "issuer":f"{AZURE_OIDC_BASE_URL}/{TEST_TENANT_ID}/v2.0"}]}


@pytest.fixture
def test_app() -> Flask:
    """mock flask app"""
    app = Flask(__name__)

    @app.route(TEST_ENDPOINT)
    @metadata.api.auth.require_auth
    def test():
        return "test"

    return app


@pytest.fixture
def test_client(test_app: Flask) -> FlaskClient:
    """mock flask test_client"""
    return test_app.test_client()


@pytest.fixture
def mock_jwt_get_unverified_header(mocker: MockerFixture) -> Mock:
    """mock jwt.get_unverified_header"""
    return mocker.patch("metadata.api.auth.jwt.get_unverified_header")


MOCK_REQUEST_GET_PATCH_PATH = "metadata.api.auth.requests.get"


@pytest.fixture
def mock_requests_get(mocker: MockerFixture) -> Mock:
    """mock requests.get"""
    return mocker.patch(MOCK_REQUEST_GET_PATCH_PATH)


@pytest.fixture
def mock_jwks_response_json_decode_error(mocker: MockerFixture) -> Mock:
    """mock jwt.decode exception"""
    class MockedResponse():
        def json(self):
            raise JSONDecodeError("Error parsing JWKS response", r"{}", 0)
    return mocker.patch(MOCK_REQUEST_GET_PATCH_PATH, return_value=MockedResponse())


@pytest.fixture
def mock_jwks_response_timeout_error(mocker: MockerFixture) -> Mock:
    """mock request.get timeout exception"""
    return mocker.patch(MOCK_REQUEST_GET_PATCH_PATH,
                        side_effect=requests.Timeout('HTTP GET Timeout'))


@pytest.fixture
def mock_jwtdecode(mocker: MockerFixture) -> Mock:
    """mock jwt.decode"""
    return mocker.patch("metadata.api.auth.jwt.decode")


@pytest.fixture
def mock_jwt_decode_jwtexception(mocker: MockerFixture) -> Mock:
    """mock jwt.decode with exception"""
    return mocker.patch("metadata.api.auth.jwt.decode", side_effect=JWTError("Signature verification failed"))


@pytest.mark.unit
def test_auth_successful(
        test_client: FlaskClient,
        mock_requests_get: Mock,
        mock_jwt_get_unverified_header: Mock,
        mock_jwtdecode: Mock):
    """Successful test case."""
    mock_jwt_get_unverified_header.return_value = {"kid": TEST_KID}
    jwks_response = Mock()
    mock_requests_get.return_value = jwks_response
    jwks_response.json = Mock(return_value=TEST_JWKS)
    mock_jwtdecode.return_value = {"iss": metadata.api.auth.AZURE_OIDC_URL, "scp": "access"}
    response = test_client.get(TEST_ENDPOINT, headers={"Authorization": TEST_AUTHORIZATION_HEADER})

    assert response.status_code == 200
    assert response.data == b"test"

@pytest.mark.unit
def test_auth_successful_multiscope(
        test_client: FlaskClient,
        mock_requests_get: Mock,
        mock_jwt_get_unverified_header: Mock,
        mock_jwtdecode: Mock):
    """Successful test case."""
    mock_jwt_get_unverified_header.return_value = {"kid": TEST_KID}
    jwks_response = Mock()
    mock_requests_get.return_value = jwks_response
    jwks_response.json = Mock(return_value=TEST_JWKS)
    mock_jwtdecode.return_value = {"iss": metadata.api.auth.AZURE_OIDC_URL, "scp": "access foobar"}
    response = test_client.get(TEST_ENDPOINT, headers={"Authorization": TEST_AUTHORIZATION_HEADER})

    assert response.status_code == 200
    assert response.data == b"test"

@pytest.mark.unit
def test_auth_fail_no_header(
        test_client: FlaskClient,
        mock_requests_get: Mock,
        mock_jwt_get_unverified_header: Mock,
        mock_jwtdecode: Mock):
    """Failed header missing test case."""
    mock_jwt_get_unverified_header.return_value = {"kid": TEST_KID}
    jwks_response = Mock()
    mock_requests_get.return_value = jwks_response
    jwks_response.json = Mock(return_value=TEST_JWKS)
    mock_jwtdecode.return_value = {"iss": metadata.api.auth.AZURE_OIDC_URL}
    response = test_client.get(TEST_ENDPOINT, headers={})

    assert response.status_code == 401


@pytest.mark.unit
def test_auth_fail_get_jwks_timeout(
        test_client: FlaskClient,
        mock_jwks_response_timeout_error: Mock,  # pylint: disable=unused-argument
        mock_jwt_get_unverified_header: Mock):
    """Failed error timeout on HTTP GET jwks url test case."""
    mock_jwt_get_unverified_header.return_value = {"kid": TEST_KID}
    response = test_client.get(TEST_ENDPOINT, headers={"Authorization": TEST_AUTHORIZATION_HEADER})

    assert response.status_code == 401


@pytest.mark.unit
def test_auth_fail_decode_jwks_json_error(
        test_client: FlaskClient,
        mock_jwt_get_unverified_header: Mock,
        mock_jwks_response_json_decode_error: Mock):  # pylint: disable=unused-argument
    """Failed error decoding jwks response JSON test case."""
    mock_jwt_get_unverified_header.return_value = {"kid": TEST_KID}
    response = test_client.get(TEST_ENDPOINT, headers={"Authorization": TEST_AUTHORIZATION_HEADER})

    assert response.status_code == 401


@pytest.mark.unit
def test_auth_fail_basic_auth(
        test_client: FlaskClient,
        mock_requests_get: Mock,
        mock_jwt_get_unverified_header: Mock,
        mock_jwtdecode: Mock):
    """Failed using basic auth test case."""
    mock_jwt_get_unverified_header.return_value = {"kid": TEST_KID}
    jwks_response = Mock()
    mock_requests_get.return_value = jwks_response
    jwks_response.json = Mock(return_value=TEST_JWKS)
    mock_jwtdecode.return_value = {"iss": metadata.api.auth.AZURE_OIDC_URL}
    response = test_client.get(TEST_ENDPOINT, headers={"Authorization": "Basic dXNlcjpwYXNz"})

    assert response.status_code == 401


@pytest.mark.unit
def test_auth_fail_kid_not_found(
        test_client: FlaskClient,
        mock_requests_get: Mock,
        mock_jwt_get_unverified_header: Mock):
    """Failed KID not found test case."""
    mock_jwt_get_unverified_header.return_value = {"kid": TEST_KID}
    jwks_response = Mock()
    mock_requests_get.return_value = jwks_response
    jwks_response.json = Mock(return_value={"keys": []})
    response = test_client.get(TEST_ENDPOINT, headers={"Authorization": TEST_AUTHORIZATION_HEADER})

    assert response.status_code == 401


@pytest.mark.unit
def test_auth_fail_missing_scope(
        test_client: FlaskClient,
        mock_requests_get: Mock,
        mock_jwt_get_unverified_header: Mock,
        mock_jwtdecode: Mock):
    """Failed wrong iss claim test case."""
    mock_jwt_get_unverified_header.return_value = {"kid": TEST_KID}
    jwks_response = Mock()
    mock_requests_get.return_value = jwks_response
    jwks_response.json = Mock(return_value=TEST_JWKS)
    mock_jwtdecode.return_value = {"iss": "foobar", "scp": ""}
    response = test_client.get(TEST_ENDPOINT, headers={"Authorization": TEST_AUTHORIZATION_HEADER})

    assert response.status_code == 401


@pytest.mark.unit
def test_auth_fail_unknown_scope(
        test_client: FlaskClient,
        mock_requests_get: Mock,
        mock_jwt_get_unverified_header: Mock,
        mock_jwtdecode: Mock):
    """Failed wrong iss claim test case."""
    mock_jwt_get_unverified_header.return_value = {"kid": TEST_KID}
    jwks_response = Mock()
    mock_requests_get.return_value = jwks_response
    jwks_response.json = Mock(return_value=TEST_JWKS)
    mock_jwtdecode.return_value = {"iss": "foobar", "scp": "foo"}
    response = test_client.get(TEST_ENDPOINT, headers={"Authorization": TEST_AUTHORIZATION_HEADER})

    assert response.status_code == 401

@pytest.mark.unit
def test_auth_fail_invalid_jwt_signature(
        test_client: FlaskClient,
        mock_requests_get: Mock,
        mock_jwt_get_unverified_header: Mock,
        mock_jwt_decode_jwtexception: Mock):
    """Failed invalid JWT signature test case."""
    mock_jwt_get_unverified_header.return_value = {"kid": TEST_KID}
    jwks_response = Mock()
    mock_requests_get.return_value = jwks_response
    jwks_response.json = Mock(return_value=TEST_JWKS)
    mock_jwt_decode_jwtexception.return_value = {"iss": "foobar"}
    response = test_client.get(TEST_ENDPOINT, headers={"Authorization": TEST_AUTHORIZATION_HEADER})

    assert response.status_code == 401
