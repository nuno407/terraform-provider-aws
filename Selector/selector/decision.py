"Decision"
from datetime import datetime, timedelta
from typing import cast
from pytz import UTC

from pydantic import validator
from pydantic.dataclasses import dataclass

from base.model.artifacts import RecorderType
from selector.exceptions import TimeInTheFuture, DatetimeObjectIsNaive, TimezoneIsNotUTC


@dataclass
class Decision:
    """Single decision outcome made by the evaluation of some ruleset"""
    recorder: RecorderType
    footage_from: datetime
    footage_to: datetime

    @validator("footage_from", "footage_to")
    def validate_footage_timestamp(cls, value: datetime):  # pylint: disable=no-self-argument
        """Validate footage_from and footage_to are valid"""
        if value > datetime.now(tz=UTC):
            raise TimeInTheFuture
        if value.utcoffset() is None:
            raise DatetimeObjectIsNaive
        if cast(timedelta, value.utcoffset()).total_seconds() != 0.0:
            raise TimezoneIsNotUTC
        return value
