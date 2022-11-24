""" Integration test. """
import pytest

# pylint: disable=missing-class-docstring,missing-function-docstring,too-few-public-methods


@pytest.mark.integration
class TestIntegrationTest:

    @staticmethod
    def test_integration_test():
        assert int("1") == 1
