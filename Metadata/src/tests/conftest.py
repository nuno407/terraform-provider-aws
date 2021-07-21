"""
This file have the responsability of
creating an mock endpoint to test the
API requests
"""
import sys
import pytest
sys.path.append('.')
from src.api import app as flask_app



@pytest.fixture
def application():
    """
    Method to run the mock api

    Arguments:
    Returns:
    """
    yield flask_app


@pytest.fixture
def client(app):
    """
    Method to run the mock api

    Arguments:
    Returns:
        ap.test_client()
    """
    return app.test_client()
