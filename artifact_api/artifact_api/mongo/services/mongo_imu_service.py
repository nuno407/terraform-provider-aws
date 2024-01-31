import logging

from kink import inject
from base.mongo.engine import Engine
from base.model.artifacts import IMUSample

from artifact_api.utils.imu_gap_finder import IMUGapFinder, TimeRange
from artifact_api.exceptions import IMUEmptyException
from artifact_api.models.mongo_models import DBIMUSample

_logger = logging.getLogger(__name__)


@inject
class MongoIMUService():

    def __init__(self, processed_imu_engine: Engine, imu_gap_finder: IMUGapFinder):
        """
        Constructor

        Args:
            processed_imu_engine (Engine): processed imu engine
        """
        self.__processed_imu_engine = processed_imu_engine
        self.__imu_gap_finder = imu_gap_finder

    async def insert_imu_data(self, imu_data: list[IMUSample]) -> list[TimeRange]:
        """
        Receives a list of IMU Samples, and inserts into the timeseries database.
        Finally returns the start and end timestamp of that IMU data.

        Args:
            imu_data: _description_
            imu_gap_finder: _description_
        """

        if len(imu_data) == 0:
            _logger.warning(
                "The imu sample list does not contain any information")
            raise IMUEmptyException()

        imu_list: list = [doc.model_dump() for doc in imu_data]

        await self.__processed_imu_engine.save_all([DBIMUSample.model_validate(imu) for imu in imu_list])
        return self.__imu_gap_finder.get_valid_imu_time_ranges(imu_list)
