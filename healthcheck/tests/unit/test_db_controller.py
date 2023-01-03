import pytest
from unittest.mock import Mock, MagicMock, call
from healthcheck.controller.db import DatabaseController, DBCollection
from healthcheck.model import SnapshotArtifact
from healthcheck.exceptions import NotYetIngestedError, NotPresentError
from datetime import datetime

@pytest.mark.unit
class TestDatabaseController():
    """unit tests for database controller."""

    @pytest.fixture
    def snap_artifact(self) -> SnapshotArtifact:
        return SnapshotArtifact(
            tenant_id="test",
            device_id="test",
            uuid="test",
            timestamp=datetime.fromisoformat("2022-12-21T14:21:44.806250")
        )

    def test_is_data_status_complete_or_raise_not_ingested(self, snap_artifact: SnapshotArtifact):
        db_client = Mock()
        db_client.find_many = Mock(return_value=[])
        schema_validator = MagicMock()
        database_controller = DatabaseController(db_client, schema_validator)
        with pytest.raises(NotYetIngestedError):
            database_controller.is_data_status_complete_or_raise(snap_artifact)

        db_client.find_many.assert_called_once_with(
            DBCollection.RECORDINGS, {
                "recording_overview.internal_message_reference_id": snap_artifact.internal_message_reference_id
            }
        )

    def test_is_data_status_complete_or_raise_not_present(self, snap_artifact: SnapshotArtifact):
        db_client = Mock()
        mocked_recording_doc = {
            "video_id": "mocked_video_id",
            "recording_overview": {
                "internal_message_reference_id": snap_artifact.internal_message_reference_id
            }
        }
        db_client.find_many = Mock(side_effect=[[mocked_recording_doc], []])
        schema_validator = MagicMock()
        database_controller = DatabaseController(db_client, schema_validator)
        with pytest.raises(NotPresentError):
            database_controller.is_data_status_complete_or_raise(snap_artifact)

        db_client.find_many.assert_has_calls(
            calls=[
                call(DBCollection.RECORDINGS, {
                    "recording_overview.internal_message_reference_id": snap_artifact.internal_message_reference_id
                }),
                call(DBCollection.PIPELINE_EXECUTION, {
                    "_id": "mocked_video_id"
                })
            ]
        )

    def test_is_data_status_complete_or_raise_not_ingested_not_complete(self, snap_artifact: SnapshotArtifact):
        db_client = Mock()
        mocked_recording_doc = {
            "video_id": "mocked_video_id",
            "recording_overview": {
                "internal_message_reference_id": snap_artifact.internal_message_reference_id
            },
        }
        mocked_pipeline_exec_doc = {
            "data_status": "pending"
        }
        db_client.find_many = Mock(side_effect=[[mocked_recording_doc], [mocked_pipeline_exec_doc]])
        schema_validator = MagicMock()
        database_controller = DatabaseController(db_client, schema_validator)
        with pytest.raises(NotYetIngestedError):
            database_controller.is_data_status_complete_or_raise(snap_artifact)

        db_client.find_many.assert_has_calls(
            calls=[
                call(DBCollection.RECORDINGS, {
                    "recording_overview.internal_message_reference_id": snap_artifact.internal_message_reference_id
                }),
                call(DBCollection.PIPELINE_EXECUTION, {
                    "_id": "mocked_video_id"
                })
            ]
        )
