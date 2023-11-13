"""conftest contains common fixtures and mocks for all unit tests"""
import sys
from unittest.mock import MagicMock

sys.modules["fiftyone"] = MagicMock()
sys.modules["fiftyone.core"] = MagicMock()
sys.modules["fiftyone.core.media"] = MagicMock()
sys.modules["fiftyone.core.metadata"] = MagicMock()
