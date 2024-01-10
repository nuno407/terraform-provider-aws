"""conftest contains common fixtures and mocks for all unit tests"""
# autopep8: off
# pylint: disable=wrong-import-position
import os
from unittest.mock import MagicMock, AsyncMock, Mock
from pytest import fixture

from base.testing.mock_functions import set_mock_aws_credentials

set_mock_aws_credentials()
os.environ["FIFTYONE_DISABLE_SERVICES"] = "1"

from base.mongo.engine import Engine
from base.testing.utils import load_relative_str_file
from base.model.artifacts.api_messages import SnapshotSignalsData
from artifact_api.api.metadata_controller import MetadataController
from artifact_api.api.media_controller import MediaController
from artifact_api.mongo_controller import MongoController
from artifact_api.utils.imu_gap_finder import IMUGapFinder
# autopep8: on


@fixture(name="imu_gap_finder")
def fixture_imu_gap_finder() -> IMUGapFinder:
    """ Fixture for IMUGapFinder

    Returns:
        IMUGapFinder: Class with methods for finding IMU Gaps
    """
    return IMUGapFinder()


@fixture(name="snapshot_engine")
def fixture_snapshot_engine() -> MagicMock:
    """ Fixture for snapshot engine

    Returns:
        MagicMock: Mock of the snapshot db engine
    """
    snapshot_engine = MagicMock()
    snapshot_engine.dump_model = Engine.dump_model
    return snapshot_engine


@fixture(name="video_engine")
def fixture_video_engine() -> MagicMock:
    """ Fixture for video engine

    Returns:
        MagicMock: Mock of the video db engine
    """
    video_engine = MagicMock()
    video_engine.dump_model = Engine.dump_model
    return video_engine


@fixture(name="event_engine")
def fixture_event_engine() -> MagicMock:
    """ Fixture for event engine

    Returns:
        MagicMock: Mock of the event db engine
    """
    return MagicMock()


@fixture(name="processed_imu_engine")
def fixture_processed_imu_engine() -> MagicMock:
    """ Fixture for processed imu engine

    Returns:
        MagicMock: Mock of the processed imu db engine
    """
    return MagicMock()


@fixture(name="operator_feedback_engine")
def fixture_operator_feedback_engine() -> MagicMock:
    """ Fixture for operator feedback engine

    Returns:
        MagicMock: Mock of the operator feedback db engine
    """
    return MagicMock()

# pylint: disable=too-many-arguments


@fixture(name="media_controller")
def fixture_generate_voxel_service() -> MediaController:
    """generate MediaController
    """
    return MediaController()


@fixture(name="metadata_controller")
def fixture_metadata_controller() -> MetadataController:
    """generate MetadataController
    """
    return MetadataController()


@fixture(name="mock_mongo_service")
def fixture_mongo_service() -> AsyncMock:
    """mocks mongo_service function
    """
    var = AsyncMock()
    var.get_correlated_snapshots_for_video = AsyncMock()
    var.upsert_video = AsyncMock()
    var.update_snapshots_correlations = AsyncMock()
    return var


@fixture(name="mock_voxel_service")
def fixture_voxel_service() -> Mock:
    """mocks voxel_service function
    """
    var = Mock()
    var.update_voxel_videos_with_correlated_snapshot = Mock()
    var.create_voxel_video = Mock()
    return var


@fixture(name="mock_voxel_metadata_transformer")
def fixture_voxel_metadata_transformer() -> Mock:
    """mocks voxel_metadata_transformer function
    """
    var = Mock()
    return var


@fixture()
def snap_signals_artifact() -> SnapshotSignalsData:
    """VideoArtifact for testing."""
    return SnapshotSignalsData.model_validate_json(load_relative_str_file(
        __file__, "test_data/training_snapshot_api_metadata_message.json"))


@fixture
def mongo_controller(event_engine: MagicMock, operator_feedback_engine: MagicMock,
                     processed_imu_engine: MagicMock, snapshot_engine: MagicMock,
                     video_engine: MagicMock, imu_gap_finder: IMUGapFinder) -> MongoController:
    """ Fixture for mongo controller

    Args:
        event_engine (MagicMock): Mock of the event engine
        operator_feedback_engine (MagicMock): Mock of the operator feedback engine
        processed_imu_engine (MagicMock): Mock of the processed imu engine
        snapshot_engine (MagicMock): Mock of the snapshot engine
        video_engine (MagicMock): Mock of the video engine

    Returns:
        MongoController: class with business logic methods for mongodb
    """
    return MongoController(event_engine=event_engine, operator_feedback_engine=operator_feedback_engine,
                           processed_imu_engine=processed_imu_engine, snapshot_engine=snapshot_engine,
                           video_engine=video_engine, imu_gap_finder=imu_gap_finder)
