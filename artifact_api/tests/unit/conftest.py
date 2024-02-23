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
from artifact_api.mongo.mongo_service import MongoService
from artifact_api.utils.imu_gap_finder import IMUGapFinder

from artifact_api.mongo.services.mongo_signals_service import MongoSignalsService
from artifact_api.mongo.services.mongo_event_service import MongoEventService
from artifact_api.mongo.services.mongo_imu_service import MongoIMUService
from artifact_api.mongo.services.mongo_sav_operator_service import MongoSavOperatorService
from artifact_api.mongo.services.mongo_recordings_service import MongoRecordingsService
from artifact_api.mongo.services.mongo_pipeline_service import MongoPipelineService
from artifact_api.mongo.services.mongo_algorithm_output_service import MongoAlgorithmOutputService
# autopep8: on


@fixture
def aggregated_metadata() -> dict[str, str | int | float | bool]:
    """Fixture for Correlated metadata"""
    return {
        "chc_duration": 5.0,
        "gnss_coverage": 0.2,
        "max_audio_loudness": 0.3,
        "max_person_count": 10,
        "mean_audio_bias": 0.5,
        "number_chc_events": 20,
        "ride_detection_people_count_after": 0,
        "ride_detection_people_count_before": 1,
        "sum_door_closed": 10,
        "variance_person_count": 0.5,
        "median_person_count": 1,
    }


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


@fixture(name="signals_engine")
def fixture_signals_engine() -> MagicMock:
    """ Fixture for signals engine

    Returns:
        MagicMock: Mock of the processed signals db engine
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


@fixture(name="pipeline_processing_status_engine")
def fixture_pipeline_processing_status_engine() -> MagicMock:
    """ Fixture for pipeline processing status engine

    Returns:
        MagicMock: Mock of the pipeline processing status db engine
    """
    return MagicMock()


@fixture(name="algorithm_output_engine")
def fixture_algorithm_output_engine() -> MagicMock:
    """ Fixture for algorithm output engine

    Returns:
        MagicMock: Mock of the algorithm output db engine
    """
    return MagicMock()


@fixture(name="mongo_pipeline_controller")
def fixture_mongo_pipeline_controller(pipeline_processing_status_engine: MagicMock) -> MongoPipelineService:
    """ Fixture for mongo pipeline controller

    Args:
        pipeline_processing_status_engine (MagicMock): Mock of the pipeline processing status db engine

    Returns:
        MongoPipelineController: class with business logic methods for mongodb
    """
    return MongoPipelineService(pipeline_processing_status_engine)


@fixture(name="mongo_recordings_controller")
def fixture_mongo_recordings_controller(video_engine: MagicMock,
                                        snapshot_engine: MagicMock) -> MongoRecordingsService:
    """ Fixture for mongo recordings controller

    Args:
        video_engine (MagicMock): Mock of the video db engine
        snapshot_engine (MagicMock): Mock of the snapshot db engine

    Returns:
        MongoRecordingsController: class with business logic methods for mongodb
    """
    return MongoRecordingsService(snapshot_engine, video_engine)


@fixture(name="mongo_sav_operator_controller")
def fixture_mongo_sav_operator_controller(operator_feedback_engine: MagicMock) -> MongoSavOperatorService:
    """ Fixture for mongo sav operator controller

    Args:
        operator_feedback_engine (MagicMock): Mock of the operator feedback db engine

    Returns:
        MongoSavOperatorController: class with business logic methods for mongodb
    """
    return MongoSavOperatorService(operator_feedback_engine)


@fixture(name="mongo_imu_controller")
def fixture_mongo_imu_controller(processed_imu_engine: MagicMock, imu_gap_finder: IMUGapFinder) -> MongoIMUService:
    """ Fixture for mongo imu controller

    Args:
        processed_imu_engine (MagicMock): Mock of the processed imu db engine

    Returns:
        MongoIMUController: class with business logic methods for mongodb
    """
    return MongoIMUService(processed_imu_engine, imu_gap_finder)


@fixture(name="mongo_signals_controller")
def fixture_mongo_signals_controller(signals_engine: MagicMock) -> MongoSignalsService:
    """ Fixture for mongo signal controller

    Args:
        signals_engine (MagicMock): Mock of the signal db engine

    Returns:
        MongoEventController: class with business logic methods for mongodb
    """
    return MongoSignalsService(signals_engine)


@fixture(name="mongo_event_controller")
def fixture_mongo_event_controller(event_engine: MagicMock) -> MongoEventService:
    """ Fixture for mongo event controller

    Args:
        event_engine (MagicMock): Mock of the event db engine

    Returns:
        MongoEventController: class with business logic methods for mongodb
    """
    return MongoEventService(event_engine)


@fixture(name="mongo_algorithm_output_controller")
def fixture_mongo_algorithm_output_controller(algorithm_output_engine: MagicMock) -> MongoAlgorithmOutputService:
    """ Fixture for mongo algorithm output controller

    Args:
        algorithm_output_engine (MagicMock): Mock of the algorithm output db engine

    Returns:
        MongoAlgorithmOutputController: class with business logic methods for mongodb
    """
    return MongoAlgorithmOutputService(algorithm_output_engine)


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
def mongo_controller(
        mongo_imu_controller: MagicMock,
        mongo_recordings_controller: MagicMock,
        mongo_sav_operator_controller: MagicMock,
        mongo_pipeline_controller: MagicMock,
        mongo_event_controller: MagicMock,
        mongo_signals_controller: MagicMock,
        mongo_algorithm_output_controller: MagicMock) -> MongoService:
    """ Fixture for mongo controller

    Args:
        mongo_imu_controller (MagicMock): Mock of the mongo imu controller
        mongo_recordings_controller (MagicMock): Mock of the mongo recordings controller
        mongo_sav_operator_controller (MagicMock): Mock of the mongo sav operator controller
        mongo_pipeline_controller (MagicMock): Mock of the mongo pipeline controller
        mongo_event_controller (MagicMock): Mock of the mongo event controller
        mongo_signals_controller (MagicMock): Mock of the mongo signals controller

    Returns:
        MongoController: class with business logic methods for mongodb
    """
    return MongoService(
        mongo_event_controller,
        mongo_imu_controller,
        mongo_sav_operator_controller,
        mongo_recordings_controller,
        mongo_pipeline_controller,
        mongo_signals_controller,
        mongo_algorithm_output_controller)
