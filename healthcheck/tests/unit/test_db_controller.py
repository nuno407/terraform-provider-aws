from datetime import datetime
from unittest.mock import MagicMock, Mock, call

import pytest
import pytz

from base.model.artifacts import RecorderType, SnapshotArtifact, VideoArtifact
from healthcheck.controller.db import DatabaseController, DBCollection
from healthcheck.exceptions import NotPresentError, NotYetIngestedError


@pytest.mark.unit
class TestDatabaseController():
    """unit tests for database controller."""

    @pytest.fixture
    def video_artifact(self) -> VideoArtifact:
        return VideoArtifact(
            tenant_id="test",
            device_id="test",
            stream_name="test_stream",
            recorder=RecorderType.INTERIOR,
            timestamp=datetime.fromisoformat("2022-12-21T14:22:44.806+00:00"),
            end_timestamp=datetime.fromisoformat("2022-12-21T14:35:44.806+00:00"),
            upload_timing={
                "start": "2022-12-21T14:22:44.806+00:00",
                "end": "2022-12-21T14:35:44.806+00:00"
            }
        )

    @pytest.fixture
    def snap_artifact(self) -> SnapshotArtifact:
        return SnapshotArtifact(
            tenant_id="test",
            device_id="test",
            uuid="test",
            recorder=RecorderType.SNAPSHOT,
            timestamp=datetime.fromisoformat("2022-12-21T14:21:44.806+00:00"),
            upload_timing={
                "start": "2022-12-21T14:21:44.806+00:00",
                "end": "2022-12-21T14:22:44.806+00:00"
            }
        )

    def test_is_data_status_complete_or_raise_not_ingested(
            self, snap_artifact: SnapshotArtifact):
        db_client = Mock()
        db_client.find_many = Mock(return_value=[])
        schema_validator = MagicMock()
        mock_logger = MagicMock()
        database_controller = DatabaseController(
            db_client, schema_validator, mock_logger)
        with pytest.raises(NotYetIngestedError):
            database_controller.is_data_status_complete_or_raise(snap_artifact)

        db_client.find_many.assert_called_once_with(
            DBCollection.RECORDINGS, {
                "recording_overview.devcloudid": snap_artifact.devcloudid
            }
        )

    def test_is_data_status_complete_or_raise_not_present(
            self, snap_artifact: SnapshotArtifact):
        db_client = Mock()
        mocked_recording_doc = {
            # this epoch timestamp at the end is important for parsing and updating it
            "video_id": "mocked_video_id_1654075244",
            "recording_overview": {
                "devcloudid": snap_artifact.devcloudid
            }
        }
        db_client.find_many = Mock(side_effect=[[mocked_recording_doc], []])
        schema_validator = MagicMock()
        database_controller = DatabaseController(
            db_client, schema_validator, MagicMock())
        original_devcloud_id = snap_artifact.devcloudid
        with pytest.raises(NotPresentError):
            database_controller.is_data_status_complete_or_raise(snap_artifact)

        db_client.find_many.assert_has_calls(
            calls=[
                call(DBCollection.RECORDINGS, {
                    "recording_overview.devcloudid": original_devcloud_id
                }),
                call(DBCollection.PIPELINE_EXECUTION, {
                    "_id": "mocked_video_id_1654075244"
                })
            ]
        )

    def test_is_data_status_complete_or_raise_not_ingested_not_complete(
            self, snap_artifact: SnapshotArtifact):
        db_client = Mock()
        mocked_recording_doc = {
            "video_id": "mocked_video_id_1654075244",
            "recording_overview": {
                "devcloudid": snap_artifact.devcloudid
            },
        }
        mocked_pipeline_exec_doc = {
            "data_status": "pending"
        }
        db_client.find_many = Mock(
            side_effect=[[mocked_recording_doc], [mocked_pipeline_exec_doc]])
        schema_validator = MagicMock()
        database_controller = DatabaseController(
            db_client, schema_validator, MagicMock())
        original_devcloud_id = snap_artifact.devcloudid
        with pytest.raises(NotYetIngestedError):
            database_controller.is_data_status_complete_or_raise(snap_artifact)

        db_client.find_many.assert_has_calls(
            calls=[
                call(DBCollection.RECORDINGS, {
                    "recording_overview.devcloudid": original_devcloud_id
                }),
                call(DBCollection.PIPELINE_EXECUTION, {
                    "_id": "mocked_video_id_1654075244"
                })
            ]
        )

    def test_is_data_status_complete_or_raise_not_ingested_not_complete_video_artifact(
            self, video_artifact: VideoArtifact):
        db_client = Mock()
        mocked_recording_doc = {
            "video_id": "mocked_video_id_1639321140000_1639321380000",
            "recording_overview": {
                "devcloudid": video_artifact.devcloudid
            },
        }
        mocked_pipeline_exec_doc = {
            "data_status": "pending"
        }
        db_client.find_many = Mock(
            side_effect=[[mocked_recording_doc], [mocked_pipeline_exec_doc]])
        schema_validator = MagicMock()
        database_controller = DatabaseController(
            db_client, schema_validator, MagicMock())
        original_devcloud_id = video_artifact.devcloudid
        with pytest.raises(NotYetIngestedError):
            database_controller.is_data_status_complete_or_raise(
                video_artifact)

        db_client.find_many.assert_has_calls(
            calls=[
                call(DBCollection.RECORDINGS, {
                    "recording_overview.devcloudid": original_devcloud_id
                }),
                call(DBCollection.PIPELINE_EXECUTION, {
                    "_id": "mocked_video_id_1639321140000_1639321380000"
                })
            ]
        )
