""" Validators for 2.x pydantic models"""
from typing import Annotated
from typing import Union
from datetime import datetime, timezone, timedelta
from pydantic import AwareDatetime
from pydantic.functional_validators import AfterValidator, BeforeValidator
from base.model.exceptions import ValidatorWrongDateTimeFormat


def check_aware_timestamp_in_past(value: AwareDatetime) -> AwareDatetime:
    """
    Ensures the timestamps are in the past

    Args:
        value (AwareDatetime): An aware timestamp

    Raises:
        ValueError: _description_

    Returns:
        AwareDatetime: The aware timestamps
    """
    cur_time = datetime.now(timezone.utc)
    if value > cur_time or value.utcoffset() != cur_time.utcoffset():  # type: ignore
        raise ValidatorWrongDateTimeFormat("timestamp must be in the past")
    return value


def convert_legacy_timedelta(value: Union[str, timedelta]) -> Union[timedelta, str]:
    """
    Converts a timedelta in the format HH:MM:SS.SSSS

    Args:
        value (str): The timedelta in string format

    Returns:
        Union[timedelta,str]: The timedelta parsed if it matches the legacy format
    """
    if not isinstance(value, str):
        return value

    time_parts = value.split(":")
    if len(time_parts) != 3:
        return value
    hours, minutes, seconds = map(float, time_parts)
    return timedelta(hours=hours, minutes=minutes, seconds=seconds)


UtcDatetimeInPast = Annotated[AwareDatetime, AfterValidator(check_aware_timestamp_in_past)]
LegacyTimeDelta = Annotated[timedelta, BeforeValidator(convert_legacy_timedelta)]
