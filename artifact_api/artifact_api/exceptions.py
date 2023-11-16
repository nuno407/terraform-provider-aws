"""artifact api exceptions module"""


class UnknowEventArtifactException(Exception):
    """_summary_

    Args:
        Exception (_type_): _description_
    """


class InvalidOperatorArtifactException(Exception):
    """The operator artifact does not match any of the accepted types"""


class IMUEmptyException(Exception):
    """The IMU sample list was empty"""
