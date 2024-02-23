"""artifact api exceptions module"""


class UnknowEventArtifactException(Exception):
    """_summary_

    Args:
        Exception (_type_): _description_
    """


class VoxelSnapshotMetadataError(Exception):
    """Error while loading the metadata of a snapshot"""


class InvalidOperatorArtifactException(Exception):
    """The operator artifact does not match any of the accepted types"""


class VoxelProcessingException(Exception):
    """The IMU sample list was empty"""


class IMUEmptyException(Exception):
    """The IMU sample list was empty"""


class FailToUpdateDocument(Exception):
    """The document cannot be updated"""
