""" constants module """
from base.model.artifacts import RecorderType

CONTAINER_NAME = "Selector"    # Name of the current container
CONTAINER_VERSION = "v2.0"      # Version of the current container

# Maps the artifact recorder names to the ones used by the footage API
FOOTAGE_RECORDER_NAME_MAP = {
    RecorderType.TRAINING: "TRAINING"
}
