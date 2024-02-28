import logging
from kink import inject
from datetime import timedelta
from base.mongo.engine import Engine
from artifact_api.models.mongo_models import DBAnonymizationResult, DBOutputPath, DBCHCResult, DBResults
from base.model.artifacts import AnonymizationResult, CHCDataResult
from base.voxel.functions import get_anonymized_path_from_raw


_logger = logging.getLogger(__name__)


@inject
class MongoAlgorithmOutputService():

    def __init__(self, algo_output_engine: Engine):
        """_summary_

        Args:
            algo_output_engine (Engine): _description_
        """
        self.__algo_output_engine = algo_output_engine

    async def save_anonymization_result_output(self, message: AnonymizationResult):
        """Saves the algorithm output to the database

        Args:
            message (AnonymizationResult): Anonymization Result
        """
        output_paths = DBOutputPath(
            video=message.s3_path.removeprefix("s3://"))

        anon_id = message.correlation_id + "_Anonymize"
        doc = DBAnonymizationResult(
            _id=anon_id,
            pipeline_id=message.correlation_id,
            output_paths=output_paths
        )

        await self.__algo_output_engine.save(doc)
        _logger.info(
            "Algorithm output has been processed successfully to mongoDB")

    async def save_chc_result_output(self, message: CHCDataResult):
        """Saves the algorithm output to the database

        Args:
            message (CHCDataResult): CHC Data Result
        """
        chc_result = message.message
        s3_path = chc_result.s3_path
        chc_id = chc_result.correlation_id + "_CHC"
        signals = message.data

        signals_dump: dict[timedelta, dict[str, int | float | bool]] = {
            k: v.model_dump() for k, v in signals.items()}
        output_paths = DBOutputPath(metadata=s3_path.removeprefix("s3://"))

        doc = DBCHCResult(
            _id=chc_id,
            pipeline_id=chc_result.correlation_id,
            output_paths=output_paths,
            results=DBResults(CHBs_sync=signals_dump)
        )

        await self.__algo_output_engine.save(doc)
        _logger.info(
            "Algorithm output has been processed successfully to mongoDB")
