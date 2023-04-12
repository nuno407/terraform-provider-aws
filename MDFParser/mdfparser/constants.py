"""Constants"""
from enum import Enum

### IMU PARSING RELATED CONSTANTS ####
MICRO_SEC = 1e-6

IMU_DELIMITER = ";"
IMU_BATCH_START_HEADER = "chunkTimeUtcMs"
IMU_BATCH_FIELDS = ["acc_x", "acc_y", "acc_z", "gyr_x", "gyr_y", "gyr_z"]

# Time between samples in microseconds (1600Hz)
IMU_SAMPLE_DELTA = int(1 / 1600 / MICRO_SEC)
######################################

IMU_TMP_SUFFIX = "_processed_imu.json"


class DataType(Enum):
    """
    The multiple DataTypes that will be handled by the Consumer class.
    Each Handler should have a diferent datatype.

    Args:
        Enum (_type_): _description_
    """
    METADATA = "metadata"
    IMU = "imu"
