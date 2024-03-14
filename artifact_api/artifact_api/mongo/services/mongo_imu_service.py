import logging

from kink import inject
from base.mongo.engine import Engine
from base.model.artifacts import IMUProcessedData

from artifact_api.utils.imu_gap_finder import IMUGapFinder, TimeRange
from artifact_api.exceptions import IMUEmptyException
from artifact_api.models.mongo_models import DBIMUSample

_logger = logging.getLogger(__name__)


@inject
class MongoIMUService():

    def __init__(self, processed_imu_engine: Engine):
        """
        Constructor

        Args:
            processed_imu_engine (Engine): processed imu engine
        """
        self.__processed_imu_engine = processed_imu_engine

    async def insert_imu_data(self, imu_data: IMUProcessedData) -> None:
        """
        Receives a list of IMU Samples, and inserts into the timeseries database.
        Finally returns the start and end timestamp of that IMU data.

        Args:
            imu_data: _description_
            imu_gap_finder: _description_
        """

        if len(imu_data) == 0:
            _logger.warning("IMU data is empty, nothing to ingest")
            return

        validated_data = imu_data.apply(lambda x: DBIMUSample.model_validate(x.to_dict()), axis=1).to_list()
        await self.__processed_imu_engine.save_all(validated_data)
