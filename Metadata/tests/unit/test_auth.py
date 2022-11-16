"""Test for API authentication module."""

from unittest.mock import Mock

import metadata.api.auth
import pytest
from cognitojwt import CognitoJWTException
from flask import Flask
from flask.testing import FlaskClient
from pytest_mock import MockerFixture

TEST_ENDPOINT = '/test'

@pytest.fixture
def test_app() -> Flask:
    app = Flask(__name__)

    @app.route(TEST_ENDPOINT)
    @metadata.api.auth.require_auth
    def test():
        return 'test'

    return app


@pytest.fixture
def test_client(test_app) -> FlaskClient:
    return test_app.test_client()

@pytest.fixture
def mock_cognitojwt(mocker: MockerFixture) -> Mock:
    return mocker.patch('metadata.api.auth.cognitojwt.decode')


@pytest.mark.unit
def test_auth_successful(test_client: FlaskClient, mock_cognitojwt: Mock):
    mock_cognitojwt.return_value = {'iss': metadata.api.auth.COGNITO_ISS}
    response = test_client.get(TEST_ENDPOINT, headers={'Authorization': 'Bearer 1234'})

    assert response.status_code == 200
    assert response.data == b'test'


@pytest.mark.unit
def test_auth_fails_on_basic(test_client: FlaskClient, mock_cognitojwt: Mock):
    mock_cognitojwt.return_value = {'iss': metadata.api.auth.COGNITO_ISS}
    response = test_client.get(TEST_ENDPOINT, headers={'Authorization': 'Basic dXNlcjpwYXNz'})

    assert response.status_code == 401


@pytest.mark.unit
def test_auth_fails_without_auth(test_client: FlaskClient, mock_cognitojwt: Mock):
    mock_cognitojwt.return_value = {'iss': metadata.api.auth.COGNITO_ISS}
    response = test_client.get(TEST_ENDPOINT)

    assert response.status_code == 401

@pytest.mark.unit
def test_auth_fails_on_wrong_issuer(test_client: FlaskClient, mock_cognitojwt: Mock):
    mock_cognitojwt.return_value = {'iss': 'foobar'}
    response = test_client.get(TEST_ENDPOINT, headers={'Authorization': 'Basic dXNlcjpwYXNz'})

    assert response.status_code == 401

@pytest.fixture
def mock_cognitojwt_exception(mocker: MockerFixture) -> Mock:
    return mocker.patch('metadata.api.auth.cognitojwt.decode',
                        side_effect=CognitoJWTException('Signature verification failed'))

@pytest.mark.unit
def test_auth_fails_without_auth(test_client: FlaskClient, mock_cognitojwt_exception: Mock):
    mock_cognitojwt.return_value = {'iss': metadata.api.auth.COGNITO_ISS}
    response = test_client.get(TEST_ENDPOINT, headers={'Authorization': 'Bearer 1234'})

    assert response.status_code == 401
