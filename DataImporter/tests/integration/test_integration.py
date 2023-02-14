""" Integration test example. """
import pytest

# pylint: disable=missing-function-docstring, missing-module-docstring, missing-class-docstring, too-few-public-methods


@pytest.mark.integration
class TestIntegrationTest:

    @staticmethod
    def test_integration_test():
        assert True
