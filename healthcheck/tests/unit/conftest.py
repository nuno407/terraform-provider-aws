"""unit tests configuration."""
import sys
import pytest
from unittest.mock import Mock
from healthcheck.config import HealthcheckConfig


@pytest.fixture
def fiftyone() -> Mock:
    fo = Mock()
    sys.modules["fiftyone"] = fo
    return fo
