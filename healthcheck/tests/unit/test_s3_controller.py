"""Test S3 controller."""
import os
from datetime import datetime

import pytest

from healthcheck.controller.aws_s3 import S3Controller
from healthcheck.exceptions import AnonymizedFileNotPresent, RawFileNotPresent
from healthcheck.model import S3Params, SnapshotArtifact, VideoArtifact
from unittest.mock import MagicMock, Mock

S3_ANON_BUCKET = "my-test-anon"
S3_RAW_BUCKET = "my-test-raw"
S3_DIR = "TEST_DIR"

@pytest.mark.unit
class TestS3Controller():
    """Test S3 controller"""

    @pytest.fixture
    def s3_params(self) -> S3Params:
        return S3Params(
            s3_bucket_anon=S3_ANON_BUCKET,
            s3_bucket_raw=S3_RAW_BUCKET,
            s3_dir=S3_DIR
        )

    @pytest.fixture
    def fix_video(self) -> VideoArtifact:
        return VideoArtifact(
            tenant_id="datanauts",
            device_id="test-device",
            stream_name="test",
            footage_from=datetime.min,
            footage_to=datetime.min)

    @pytest.fixture
    def fix_snap(self) -> SnapshotArtifact:
        return SnapshotArtifact(
            tenant_id="datanauts",
            device_id="test-device",
            uuid="foobar",
            timestamp=datetime.min)

    def test_is_s3_anonymized_file_present_or_raise_success(self, fix_video: VideoArtifact, s3_params: S3Params):
        fix_test_client = Mock()
        fix_test_client.head_object = Mock(return_value={"ResponseMetadata": {"HTTPStatusCode": 200}})
        s3_controller = S3Controller(s3_params, fix_test_client)
        s3_controller.is_s3_anonymized_file_present_or_raise("mock-anon.mp4", fix_video)
        fix_test_client.head_object.assert_called_once_with(Bucket=S3_ANON_BUCKET, Key=f"{S3_DIR}/mock-anon.mp4")

    def test_is_s3_anonymized_file_present_or_raise_fail(self, fix_video: VideoArtifact, s3_params: S3Params):
        fix_test_client = MagicMock()
        fix_test_client.head_object = Mock(side_effect=AnonymizedFileNotPresent(fix_video, "test"))
        s3_controller = S3Controller(s3_params, fix_test_client)
        with pytest.raises(AnonymizedFileNotPresent):
            s3_controller.is_s3_anonymized_file_present_or_raise("foobar.mp4", fix_video)

    def test_is_s3_raw_file_presence_or_raise_success(self, fix_snap: SnapshotArtifact, s3_params: S3Params):
        fix_test_client = MagicMock()
        s3_controller = S3Controller(s3_params, fix_test_client)
        fix_test_client.head_object = Mock(return_value={"ResponseMetadata": {"HTTPStatusCode": 200}})
        s3_controller.is_s3_raw_file_presence_or_raise("mock-raw.jpeg", fix_snap)
        fix_test_client.head_object.assert_called_once_with(Bucket=S3_RAW_BUCKET, Key=f"{S3_DIR}/mock-raw.jpeg")

    def test_is_s3_raw_file_presence_or_raise_success_fail(self, fix_snap: SnapshotArtifact, s3_params: S3Params):
        fix_test_client = MagicMock()
        fix_test_client.head_object = Mock(side_effect=RawFileNotPresent(fix_snap, "test"))
        s3_controller = S3Controller(s3_params, fix_test_client)
        with pytest.raises(RawFileNotPresent):
            s3_controller.is_s3_raw_file_presence_or_raise("foobar.jpeg", fix_snap)
