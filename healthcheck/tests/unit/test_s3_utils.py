"""Test S3 controller."""
from datetime import datetime
from unittest.mock import MagicMock, Mock

import pytest
from pytz import UTC

from base.aws.s3 import S3Controller
from base.model.artifacts import (RecorderType, S3VideoArtifact,
                                  SnapshotArtifact, TimeWindow)
from healthcheck.exceptions import AnonymizedFileNotPresent, RawFileNotPresent
from healthcheck.model import S3Params
from healthcheck.s3_utils import S3Utils

S3_ANON_BUCKET = "my-test-anon"
S3_RAW_BUCKET = "my-test-raw"


@pytest.mark.unit
class TestS3Utils:
    """Test S3 utils"""

    @pytest.fixture
    def s3_params(self) -> S3Params:
        return S3Params(
            s3_bucket_anon=S3_ANON_BUCKET,
            s3_bucket_raw=S3_RAW_BUCKET
        )

    @pytest.fixture
    def fix_video(self) -> S3VideoArtifact:
        return S3VideoArtifact(
            tenant_id="datanauts",
            device_id="test-device",
            footage_id="test",
            rcc_s3_path="s3://bucket/key",
            recorder=RecorderType.INTERIOR,
            timestamp=datetime.now(tz=UTC),
            end_timestamp=datetime.now(tz=UTC),
            upload_timing=TimeWindow(
                start=datetime.now(tz=UTC),
                end=datetime.now(tz=UTC)
            ))

    @pytest.fixture
    def fix_snap(self) -> SnapshotArtifact:
        return SnapshotArtifact(
            tenant_id="datanauts",
            device_id="test-device",
            uuid="foobar",
            recorder=RecorderType.SNAPSHOT,
            timestamp=datetime.now(tz=UTC),
            upload_timing=TimeWindow(
                start=datetime.now(tz=UTC),
                end=datetime.now(tz=UTC)
            ))

    def test_is_s3_anonymized_file_present_or_raise_success(self, fix_video: S3VideoArtifact, s3_params: S3Params):
        fix_test_client = Mock()
        fix_test_client.head_object = Mock(
            return_value={"ResponseMetadata": {"HTTPStatusCode": 200}})
        s3_controller = S3Controller(fix_test_client)
        s3_utils = S3Utils(s3_params, s3_controller)
        s3_utils.is_s3_anonymized_file_present_or_raise(
            "mock-anon.mp4", fix_video)
        fix_test_client.head_object.assert_called_once_with(
            Bucket=S3_ANON_BUCKET, Key=f"{fix_video.tenant_id}/mock-anon.mp4")

    def test_is_s3_anonymized_file_present_or_raise_fail(self, fix_video: S3VideoArtifact, s3_params: S3Params):
        fix_test_client = MagicMock()
        fix_test_client.head_object = Mock(
            side_effect=AnonymizedFileNotPresent(fix_video, "test"))
        s3_controller = S3Controller(fix_test_client)
        s3_utils = S3Utils(s3_params, s3_controller)
        with pytest.raises(AnonymizedFileNotPresent):
            s3_utils.is_s3_anonymized_file_present_or_raise(
                "foobar.mp4", fix_video)

    def test_is_s3_raw_file_presence_or_raise_success(self, fix_snap: SnapshotArtifact, s3_params: S3Params):
        fix_test_client = MagicMock()
        s3_controller = S3Controller(fix_test_client)
        s3_utils = S3Utils(s3_params, s3_controller)
        fix_test_client.head_object = Mock(
            return_value={"ResponseMetadata": {"HTTPStatusCode": 200}})
        s3_utils.is_s3_raw_file_present_or_raise(
            "mock-raw.jpeg", fix_snap)
        fix_test_client.head_object.assert_called_once_with(
            Bucket=S3_RAW_BUCKET, Key=f"{fix_snap.tenant_id}/mock-raw.jpeg")

    def test_is_s3_raw_file_presence_or_raise_success_fail(self, fix_snap: SnapshotArtifact, s3_params: S3Params):
        fix_test_client = MagicMock()
        fix_test_client.head_object = Mock(
            side_effect=RawFileNotPresent(fix_snap, "test"))
        s3_controller = S3Controller(fix_test_client)
        s3_utils = S3Utils(s3_params, s3_controller)
        with pytest.raises(RawFileNotPresent):
            s3_utils.is_s3_raw_file_present_or_raise(
                "foobar.jpeg", fix_snap)
