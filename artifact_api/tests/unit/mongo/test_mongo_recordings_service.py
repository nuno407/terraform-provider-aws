"""Tests for mongodb signals service"""

from unittest.mock import AsyncMock, MagicMock
from pytest import fixture, mark
from base.model.validators import LegacyTimeDelta
from base.model.artifacts.api_messages import SignalsFrame

from artifact_api.models.mongo_models import DBSignals
from artifact_api.mongo.services.mongo_recordings_service import MongoRecordingsService


@mark.unit
class TestRecordingsMongoService:
    """Class with tests for mongodb controller"""

    @mark.unit
    async def test_upsert_video_aggregated_metadata(self, aggregated_metadata: dict[str, str | int | float | bool], mongo_recordings_controller: MongoRecordingsService, video_engine: MagicMock):
        """Test for upsert agregated metadata method

        Args:
            snapshot_engine (MagicMock): snapshot engine mock
            mongo_controller (MongoController): class where the tested method is defined
            snapshot_artifact (SnapshotArtifact): snapshot artifact to be ingested
        """
        # GIVEN
        video_engine.update_one_flatten = AsyncMock()
        correlated_id = "id_1"
        recording_overview = {
            "snapshots_paths": [],
            "#snapshots": 0
        }
        recording_overview.update(aggregated_metadata)
        query = {
            "video_id": correlated_id,
            "_media_type": "video"
        }
        command = {
            "video_id": correlated_id,
            "MDF_available": "Yes",
            "_media_type": "video",
            "recording_overview": recording_overview
        }

        # THEN
        await mongo_recordings_controller.upsert_video_aggregated_metadata(aggregated_metadata, correlated_id)

        # THEN
        video_engine.update_one_flatten.assert_called_once_with(query=query, set_command=command, upsert=True)
