from unittest.mock import Mock

import pytest

from sanitizer.main import main

@pytest.mark.unit
def test_main_method():
    handler = Mock()
    handler.run = Mock(return_value=None)
    main(handler)
    handler.run.assert_called_once()
