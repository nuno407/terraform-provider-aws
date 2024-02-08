"""Tests for mongodb signals service"""

from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock
from pytest import fixture, mark
from base.model.validators import LegacyTimeDelta
from base.model.artifacts.api_messages import SignalsFrame

from artifact_api.models.mongo_models import DBSignals
from artifact_api.mongo.services.mongo_signals_service import MongoSignalsService


@mark.unit
class TestMongoController:  # pylint: disable=duplicate-code
    """Class with tests for mongodb controller"""

    @fixture
    def output(self) -> DBSignals:
        """Fixture for correlated videos"""
        return DBSignals(source="MDFParser", recording="recording_id", signals={
            timedelta(seconds=0): {
                "CameraViewShifted": False,
                "CameraViewShiftedConfidence": 0.1,
            },
            timedelta(microseconds=63333): {
                "CameraViewShifted": False,
                "CameraViewShiftedConfidence": 0.1,
            }
        })

    @mark.unit
    @mark.parametrize("source", ["MDFParser", "CHC"])
    async def test_upsert_snapshot(self, output: DBSignals, mongo_signals_controller: MongoSignalsService,
                                   source: str,
                                   signals_engine: MagicMock):
        """Test for upsert_snapshot method

        Args:
            snapshot_engine (MagicMock): snapshot engine mock
            mongo_controller (MongoController): class where the tested method is defined
            snapshot_artifact (SnapshotArtifact): snapshot artifact to be ingested
        """
        # GIVEN
        output.source = source
        signals_engine.save = AsyncMock()
        recording = "recording_id"
        db_signals = {k:SignalsFrame.model_validate(v) for k,v in output.signals.items()}

        # THEN
        await mongo_signals_controller.save_signals(db_signals, source, recording)

        # THEN
        signals_engine.save.assert_called_once_with(output)
