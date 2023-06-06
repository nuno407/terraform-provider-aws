"""Exceptions module"""


class RecorderNotImplemented(Exception):
    """Requested recorder is does not have a mapping to it's the Footage API name"""


class InvalidDecision(Exception):
    """Decision violates some DevCloud ruling"""


class InvariantViolation(Exception):
    """'Hard line' was violated"""


class TimeInTheFuture(Exception):
    """No decisions can be made about future data"""


class DatetimeObjectIsNaive(Exception):
    """Datetime object is naive about its timezone"""
    # https://docs.python.org/3/library/datetime.html#determining-if-an-object-is-aware-or-naive


class TimezoneIsNotUTC(Exception):
    """All datetime objects must have 'UTC' as timezone"""
    # https://docs.python.org/3/library/datetime.html#determining-if-an-object-is-aware-or-naive
