import logging

from kink import inject
from base.mongo.engine import Engine
from artifact_api.models.mongo_models import DBPipelineProcessingStatus, DBAnonymizationResult, DBOutputPath

from base.model.artifacts import PipelineProcessingStatus, AnonymizationResult
from base.model.artifacts.api_messages import CHCDataResult

_logger = logging.getLogger(__name__)


@inject
class MongoPipelineService():

    def __init__(self, pipeline_processing_status_engine: Engine):
        """
        Constructor

        Args:
            pipeline_processing_status_engine (Engine): pipeline processing status engine
        """
        self.__pipeline_processing_status_engine = pipeline_processing_status_engine

    async def save_pipeline_processing_status(self, message: PipelineProcessingStatus, last_updated: str):
        """Creates DB Pipeline Processing Status Artifact and writes the document to the mongoDB

        Args:
            message (PipelineProcessingStatus): Pipeline Processing Status Artifact
            last_updated (str): Last Updated date as a string

        Returns:
            DBPipelineProcessingStatus: Corresponding DB Pipeline Processing Status Artifact
        """
        doc = DBPipelineProcessingStatus(
            _id=message.correlation_id,
            s3_path=message.s3_path,
            artifact_name=message.artifact_name,
            info_source=message.info_source,
            last_updated=last_updated,
            processing_status=message.processing_status,
            processing_list=message.processing_steps
        )

        await self.__pipeline_processing_status_engine.update_one(
            query={
                "_id": message.correlation_id
            },
            command={
                # in case that the snapshot is not created we add the missing fields
                "$set": self.__pipeline_processing_status_engine.dump_model(doc)
            },
            upsert=True
        )
        return doc

    async def update_pipeline_processing_status_anonymization(self, message: AnonymizationResult, last_updated: str):
        """Updates the pipeline processing status

        Args:
            message (PipelineProcessingStatus): Pipeline Processing Status Artifact
            last_updated (str): Last Updated date as a string
        """
        await self.__pipeline_processing_status_engine.update_one(
            query={
                "_id": message.correlation_id
            },
            command={
                "$set": {
                    "last_updated": last_updated,
                    "processing_status": message.processing_status,
                    "info_source": "Anonymize"
                }
            }
        )
        _logger.info(
            "Pipeline processing status has been updated successfully to mongoDB")

    async def update_pipeline_processing_status_chc(self, message: CHCDataResult, last_updated: str):
        """Updates the pipeline processing status

        Args:
            message (PipelineProcessingStatus): Pipeline Processing Status Artifact
            last_updated (str): Last Updated date as a string
        """
        chc_result = message.message
        await self.__pipeline_processing_status_engine.update_one(
            query={
                "_id": chc_result.correlation_id
            },
            command={
                "$set": {
                    "last_updated": last_updated,
                    "processing_status": chc_result.processing_status,
                    "info_source": "CHC"
                }
            }
        )
        _logger.info(
            "Pipeline processing status has been updated successfully to mongoDB")
