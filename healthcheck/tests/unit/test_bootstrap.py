from unittest.mock import Mock, patch, call

import pytest

from healthcheck.bootstrap import EnvironmentParams, bootstrap_di, get_environment


@pytest.mark.unit
class TestBootstrap:
    @patch("healthcheck.bootstrap.os")
    def test_get_environment(self, os_mock: Mock):
        os_mock.getenv = Mock(side_effect=[
            "test1",
            "us-east-1",
            "development",
            "config.yaml",
            "databaseurimock",
            "webhookmock"
        ])
        params = get_environment()
        assert isinstance(params, EnvironmentParams)

        assert params.aws_endpoint == "test1"
        assert params.aws_region == "us-east-1"
        assert params.container_version == "development"
        assert params.config_path == "config.yaml"
        assert params.db_uri == "databaseurimock"
        assert params.webhook_url == "webhookmock"

        os_mock.getenv.assert_has_calls(
            calls=[
                call("AWS_ENDPOINT", None),
                call("AWS_REGION", "eu-central-1"),
                call("CONTAINER_VERSION", "development"),
                call("CONFIG_PATH", "/app/config/config.yml"),
                call("FIFTYONE_DATABASE_URI"),
                call("MSTEAMS_WEBHOOK", "")
            ]
        )
