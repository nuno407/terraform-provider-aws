"""Metadata API integration tests."""
# pylint: disable=missing-function-docstring,missing-module-docstring,missing-class-docstring
import json
import os
import sys
from functools import wraps
from typing import Callable
from unittest.mock import MagicMock, Mock

from flask.testing import FlaskClient
import mongomock
import pytest
from pytest_mock import MockerFixture
from metadata.api.controller import init_controller
from metadata.api.db import Persistence
from metadata.api.service import ApiService

from tests.common import db_tables

sys.modules["api.config"] = MagicMock()

mockdb_client = mongomock.MongoClient()
recordings = mockdb_client.DataIngestion.recordings
signals = mockdb_client.DataIngestion.signals
pipeline_execs = mockdb_client.DataIngestion.pipeline_exec
algo_outputs = mockdb_client.DataIngestion.algo_output
persistence = Persistence(None, db_tables, mockdb_client)
s3mock = MagicMock()
service = ApiService(persistence, s3mock)

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))
testrecordings = json.loads(
    open(os.path.join(__location__, "test_data/recordings.json"), "r").read())
testexecs = json.loads(open(os.path.join(
    __location__, "test_data/pipeline_executions.json"), "r").read())
testalgo = json.loads(
    open(os.path.join(__location__, "test_data/algo_output.json"), "r").read())
recordings.insert_many(testrecordings)
pipeline_execs.insert_many(testexecs)
algo_outputs.insert_one(testalgo)


@pytest.mark.integration
class TestApi:

    def mock_auth(self, api_method: Callable):
        @wraps(api_method)
        def auth_true(*args, **kwargs):
            return api_method(*args, **kwargs)
        return auth_true

    @pytest.fixture(autouse=True)
    def no_auth(self, mocker: MockerFixture):
        mocker.patch("metadata.api.controller.require_auth", self.mock_auth)

    @pytest.fixture
    def client_mock(self):
        """Flask mocked client fixture."""
        app = init_controller(service)
        app.testing = True
        with app.test_client() as flask_client:
            return flask_client

    @pytest.mark.integration
    @pytest.mark.parametrize("video_url,s3_bucket_name,s3_key",
                             [("/getAnonymizedVideoUrl/srxdriverpr1external07_rc_srx_qa_driverpr1_007_InteriorRecorder_1645517710980_1645519350202",  # pylint: disable=line-too-long
                               "qa-rcd-anonymized-video-files",
                               "Debug_Lync/srxdriverpr1external07_rc_srx_qa_driverpr1_007_InteriorRecorder_1645517710980_1645519350202_anonymized.mp4"),  # pylint: disable=line-too-long
                              ])
    def test_get_video_url(self, client_mock: FlaskClient, video_url: str, s3_bucket_name: str, s3_key: str):
        s3mock.generate_presigned_url = Mock(return_value="demoUrl")

        resp = client_mock.get(video_url)

        assert resp.status_code == 200
        data = json.loads(resp.data)
        assert data["message"] == "demoUrl"
        assert data["statusCode"] == "200"

        s3mock.generate_presigned_url.assert_called_once()
        args = s3mock.generate_presigned_url.call_args.args
        assert args[0] == "get_object"
        kwargs = s3mock.generate_presigned_url.call_args.kwargs
        params = kwargs["Params"]
        assert params["Bucket"] == s3_bucket_name
        assert params["Key"] == s3_key
