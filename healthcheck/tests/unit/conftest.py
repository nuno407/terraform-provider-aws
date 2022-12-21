"""unit tests configuration."""
import sys
import pytest
from unittest.mock import Mock

@pytest.fixture
def fiftyone() -> Mock:
    fo = Mock()
    sys.modules["fiftyone"] = fo
    return fo
