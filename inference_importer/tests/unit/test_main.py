"""Test main"""
from unittest import mock
from unittest.mock import Mock

import pytest

from inference_importer.main import main

# pylint: disable=missing-function-docstring, missing-module-docstring, missing-class-docstring, too-few-public-methods
# pylint: disable=redefined-outer-name


@pytest.mark.unit
class TestMain:

    @mock.patch("boto3.client")
    @mock.patch("inference_importer.main.ContainerServices")
    @mock.patch("inference_importer.main.process_message")
    def test_main(self, process_message: Mock, _container_services, client: Mock):
        # GIVEN
        _stop_condition = Mock(side_effect=[True, False])

        # WHEN
        main(stop_condition=_stop_condition)

        # THEN
        client.assert_called()
        client.assert_called()
        assert 2 == client.call_count
        process_message.assert_called_once()
