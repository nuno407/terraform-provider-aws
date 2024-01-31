""" constants module """
from base.model.artifacts import RecorderType

CONTAINER_NAME = "Selector"    # Name of the current container
CONTAINER_VERSION = "v2.0"      # Version of the current container

ALLOWED_METADATA_VERSIONS = ["0.6", "2."]
# Maps the artifact recorder names to the ones used by the footage API
FOOTAGE_RECORDER_NAME_MAP = {
    RecorderType.TRAINING: "TRAINING",
    RecorderType.SNAPSHOT: "TRAINING_MULTI_SNAPSHOT"
}
