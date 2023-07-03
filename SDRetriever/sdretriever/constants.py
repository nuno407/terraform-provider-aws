"""constants module"""
from enum import Enum

CONTAINER_NAME = "SDRetriever"
CONTAINER_VERSION = "v7"
MESSAGE_VISIBILITY_EXTENSION_HOURS = [0.5, 3, 12, 12]
TRAINING_RECORDER = "TrainingRecorder"
FRONT_RECORDER = "FrontRecorder"
INTERIOR_RECORDER = "InteriorRecorder"
SNAPSHOT = "TrainingMultiSnapshot"
INTERIOR_RECORDER_PREVIEW = "InteriorRecorderPreview"


class FileExt(Enum):
    """ File extensions """
    METADATA = ".json"
    SNAPSHOT = ".jpeg"
    VIDEO = ".mp4"
