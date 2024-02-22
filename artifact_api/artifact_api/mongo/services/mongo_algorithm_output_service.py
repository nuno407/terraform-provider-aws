import logging
from kink import inject
from base.mongo.engine import Engine
from artifact_api.models.mongo_models import DBAnonymizationResult, DBOutputPath
from base.model.artifacts import AnonymizationResult


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
