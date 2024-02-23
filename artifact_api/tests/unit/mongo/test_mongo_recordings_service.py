"""Tests for mongodb signals service"""

from unittest.mock import AsyncMock, MagicMock
from pytest import fixture, mark
from base.model.validators import LegacyTimeDelta
from base.model.artifacts.api_messages import SignalsFrame

from artifact_api.models.mongo_models import DBSignals
from artifact_api.mongo.services.mongo_recordings_service import MongoRecordingsService
from artifact_api.models.mongo_models import (DBS3VideoArtifact, DBSnapshotArtifact, DBSnapshotUploadRule,
                                              DBVideoRecordingOverview, DBSnapshotRecordingOverview, DBVideoUploadRule)


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

        recording_overview = DBVideoRecordingOverview(aggregated_metadata=aggregated_metadata)
        video_model = DBS3VideoArtifact(
            video_id=correlated_id,
            MDF_available="Yes",
            media_type="video",
            recording_overview=recording_overview,
        )
        query = {
            "video_id": correlated_id,
            "_media_type": "video"
        }

        # THEN
        await mongo_recordings_controller.upsert_video_aggregated_metadata(aggregated_metadata, correlated_id)

        # THEN
        video_engine.update_one_flatten.assert_called_once_with(query=query, set_command=video_model, upsert=True)
