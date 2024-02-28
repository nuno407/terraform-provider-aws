""" Module with tests for mongodb algo output service """

from pytest import fixture, mark
from datetime import timedelta
from unittest.mock import MagicMock, AsyncMock, patch
from artifact_api.models.mongo_models import DBAnonymizationResult, DBOutputPath, DBCHCResult, DBResults
from base.model.artifacts import AnonymizationResult, CHCResult
from base.model.artifacts.api_messages import CHCDataResult
from artifact_api.mongo.services.mongo_algorithm_output_service import MongoAlgorithmOutputService


@mark.unit
class TestMongoController:  # pylint: disable=duplicate-code
    """Class with tests for mongodb controller"""

    @fixture
    def output(self) -> DBAnonymizationResult:
        """Fixture for anonymization result"""
        return DBAnonymizationResult(
            _id="correlation_id_Anonymize",
            pipeline_id="correlation_id",
            output_paths=DBOutputPath(video="video_path/key.mp4")
        )

    @fixture
    def chc_output(self) -> DBCHCResult:
        """Fixture for chc result"""
        return DBCHCResult(
            _id="correlation_id_CHC",
            pipeline_id="correlation_id",
            output_paths=DBOutputPath(metadata="video_path/key.mp4"),
            results=DBResults(CHBs_sync={
                timedelta(seconds=0): {
                        "CameraViewShifted": False,
                        "CameraViewShiftedConfidence": 0.1,
                },
                timedelta(seconds=0.3): {
                    "CameraViewShifted": False,
                    "CameraViewShiftedConfidence": 0.1,
                }
            })
        )

    @mark.unit
    async def test_save_anonymization_result_output(self,
                                                    output: DBAnonymizationResult,
                                                    mongo_algorithm_output_controller: MongoAlgorithmOutputService,
                                                    algorithm_output_engine: MagicMock):
        """Test for save_anonymization_result_output method

        Args:
            algorithm_output_engine (MagicMock): algo output engine mock
            mongo_algorithm_output_controller (MongoAlgorithmOutputService): class where the tested method is defined
            message (AnonymizationResult): message to be saved
        """
        # GIVEN
        algorithm_output_engine.save = AsyncMock()
        anonymization_result = AnonymizationResult(
            correlation_id="correlation_id",
            s3_path="s3://video_path/key.mp4",
            raw_s3_path="s3://video_path/key.mp4",
            tenant_id="tenant_id",
            processing_status="processing",
        )
        # THEN
        await mongo_algorithm_output_controller.save_anonymization_result_output(anonymization_result)

        # THEN
        algorithm_output_engine.save.assert_called_once_with(output)

    @mark.unit
    async def test_save_chc_data_result_output(self,
                                               mongo_algorithm_output_controller: MongoAlgorithmOutputService,
                                               algorithm_output_engine: MagicMock,
                                               chc_output):
        """Test for save_chc_data_result_output method

        Args:
            algorithm_output_engine (MagicMock): algo output engine mock
            mongo_algorithm_output_controller (MongoAlgorithmOutputService): class where the tested method is defined
            message (CHCDataResult): message to be saved
        """
        # GIVEN
        algorithm_output_engine.save = AsyncMock()
        chc_data_result = CHCDataResult(
            correlation_id="correlation_id",
            message=CHCResult(
                correlation_id="correlation_id",
                s3_path="s3://video_path/key.mp4",
                tenant_id="tenant_id",
                processing_status="processing",
                raw_s3_path="s3://video_path/key.mp4",
            ),
            data={
                timedelta(seconds=0): {
                    "CameraViewShifted": False,
                    "CameraViewShiftedConfidence": 0.1,
                },
                timedelta(seconds=0.3): {
                    "CameraViewShifted": False,
                    "CameraViewShiftedConfidence": 0.1,
                }
            }
        )

        # THEN
        await mongo_algorithm_output_controller.save_chc_result_output(chc_data_result)

        # THEN
        algorithm_output_engine.save.assert_called_once()
        algorithm_output_engine.save.assert_called_once_with(chc_output)
