import pytest
from unittest.mock import Mock, patch
from healthcheck.main import main

@pytest.mark.unit
class TestMain:
    @patch("healthcheck.main.start")
    def test_main(self, start_mock: Mock):
        main()
        start_mock.assert_called_once()
