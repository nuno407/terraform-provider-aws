"""conftest contains common fixtures and mocks for all unit tests"""
import os

import sys
from unittest.mock import MagicMock

from base.testing.utils import get_abs_path
from data_importer.bootstrap import bootstrap_di

sys.modules["fiftyone"] = MagicMock()
sys.modules["fiftyone.core.metadata"] = MagicMock()

os.environ["TENANT_MAPPING_CONFIG_PATH"] = get_abs_path(__file__, "data/config.yml")
bootstrap_di()
