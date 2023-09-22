"""constants module"""
from enum import Enum
import re

CONTAINER_NAME = "SDRetriever"
CONTAINER_VERSION = "v7"
MESSAGE_VISIBILITY_EXTENSION_HOURS = [0.1, 0.5, 2, 24]
TRAINING_RECORDER = "TrainingRecorder"
FRONT_RECORDER = "FrontRecorder"
INTERIOR_RECORDER = "InteriorRecorder"
SNAPSHOT = "TrainingMultiSnapshot"
INTERIOR_RECORDER_PREVIEW = "InteriorRecorderPreview"


class FileExtension(str, Enum):
    """ File extensions """
    METADATA = ".json"
    ZIPPED_METADATA = ".json.zip"
    SNAPSHOT = ".jpeg"
    VIDEO = ".mp4"
    IMU = ".csv"

    def __str__(self) -> str:
        """
        Return the name of the field (removes the need of calling .value)

        Returns:
            str: The field name
        """
        return str.__str__(self)


VIDEO_CHUNK_REGX = re.compile(r"([^\W_]+)_([^\W_]+)-([a-z0-9\-]+)_(\d+)\.mp4$")
SNAPSHOT_CHUNK_REGX = re.compile(
    r"([^\W_]+)_([^\W_]+)-([a-z0-9\-]+)_(\d+)\.jpeg$")
METADATA_CHUNK_REGX = re.compile(
    r"([^\W_]+_[^\W_]+-[a-z0-9\-]+_\d+\.jpeg).+\.json$")
