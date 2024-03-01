"Decision"
from datetime import datetime, timedelta, timezone
from typing import cast

from pydantic import field_validator
from pydantic.dataclasses import dataclass

from base.model.artifacts import RecorderType
from selector.exceptions import TimeInTheFuture, DatetimeObjectIsNaive, TimezoneIsNotUTC


@dataclass
class Decision:
    """Single decision outcome made by the evaluation of some ruleset"""
    rule_name: str
    rule_version: str
    recorder: RecorderType
    footage_from: datetime
    footage_to: datetime

    @field_validator("footage_from", "footage_to")
    @classmethod
    def validate_footage_timestamp(cls, value: datetime):  # pylint: disable=no-self-argument
        """Validate footage_from and footage_to are valid"""
        if value > datetime.now(timezone.utc):
            raise TimeInTheFuture
        if value.utcoffset() is None:
            raise DatetimeObjectIsNaive
        if cast(timedelta, value.utcoffset()).total_seconds() != 0.0:
            raise TimezoneIsNotUTC
        return value
