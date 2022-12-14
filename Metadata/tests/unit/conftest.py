"""conftest contains common fixtures and mocks for all unit tests"""
import sys
from unittest.mock import Mock
sys.modules["fiftyone"] = Mock()
