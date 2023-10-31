"Unit tests for config"
import os
import pytest
from base.testing.utils import get_abs_path
from artifact_api.config import ArtifactAPIConfig


class TestConfig:  # pylint: disable=too-few-public-methods
    "Unit tests for config"

    @pytest.mark.unit
    def test_post_endpoint_exist(self):
        """Test config parsing"""
        rel_file_path = os.path.join("test_data", "config_test.yaml")
        config = ArtifactAPIConfig.load_yaml_config(get_abs_path(__file__, rel_file_path))
        assert config.mongodb_name == "Testdb"
