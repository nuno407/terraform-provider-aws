"""conftest contains common fixtures and mocks for all unit tests"""
import sys
from unittest.mock import MagicMock

sys.modules["fiftyone"] = MagicMock()
sys.modules["fiftyone.server.view"] = MagicMock()
sys.modules["fiftyone.core.view"] = MagicMock()
