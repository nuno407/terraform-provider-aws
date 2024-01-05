"Unit tests for metadata_controller"
from unittest.mock import Mock
import pytest
from base.model.artifacts import SnapshotSignalsData
from artifact_api.models import ResponseMessage
from artifact_api.api.metadata_controller import MetadataController


class TestMetadataController:  # pylint: disable=too-few-public-methods
    "Unit tests for metadata_controller endpoints"

    @pytest.mark.unit
    async def test_process_video_artifact(self, snap_signals_artifact: SnapshotSignalsData,
                                          mock_voxel_service: Mock,
                                          metadata_controller: MetadataController):
        """
        Test a snapshot metadata process

        """
        # WHEN
        result = await metadata_controller.process_snapshots_signals(snap_signals_artifact, mock_voxel_service)
        # THEN
        mock_voxel_service.load_snapshot_metadata.assert_called_once_with(snap_signals_artifact)
        assert result == ResponseMessage()
