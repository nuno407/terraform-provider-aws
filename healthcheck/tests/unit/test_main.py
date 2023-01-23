from unittest.mock import Mock, MagicMock, patch

import pytest

from healthcheck.main import main, start


@pytest.mark.unit
class TestMain:
    @patch("healthcheck.main.start")
    def test_main(self, start_mock: Mock):
        main()
        start_mock.assert_called_once()

    def test_start(self):
        mocked_worker = Mock()
        mocked_worker.run = Mock()
        start(mocked_worker, "1", MagicMock())
        mocked_worker.run.assert_called_once()
