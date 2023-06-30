"""conftest contains common fixtures and mocks for all unit tests"""
import sys
from unittest.mock import MagicMock

from kink import di

from data_importer.models.config import DatasetMappingConfig

sys.modules["fiftyone"] = MagicMock()
sys.modules["fiftyone.core.metadata"] = MagicMock()


# Injecting config for tests
config = DatasetMappingConfig(
    create_dataset_for={"datanauts"},
    default_dataset="Debug_Lync",
    tag="RC",
    default_policy_document="default-policy",
    policy_document_per_tenant={
        "test-tenant": "test-policy"})
di[DatasetMappingConfig] = config
