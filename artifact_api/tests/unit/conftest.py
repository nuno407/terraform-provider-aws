"""conftest contains common fixtures and mocks for all unit tests"""

import sys
from unittest.mock import MagicMock
from pytest import fixture

from artifact_api.utils.imu_gap_finder import IMUGapFinder
from artifact_api.mongo_controller import MongoController

sys.modules["fiftyone"] = MagicMock()
sys.modules["fiftyone.core"] = MagicMock()
sys.modules["fiftyone.core.media"] = MagicMock()
sys.modules["fiftyone.core.metadata"] = MagicMock()


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
    return MagicMock()


@fixture(name="video_engine")
def fixture_video_engine() -> MagicMock:
    """ Fixture for video engine

    Returns:
        MagicMock: Mock of the video db engine
    """
    return MagicMock()


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
