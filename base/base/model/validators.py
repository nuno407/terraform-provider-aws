""" Validators for 2.x pydantic models"""
from typing import Annotated
from datetime import datetime, timezone
from pydantic import AwareDatetime
from pydantic.functional_validators import AfterValidator


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
        raise ValueError("timestamp must be in the past")
    return value


UtcDatetimeInPast = Annotated[AwareDatetime, AfterValidator(check_aware_timestamp_in_past)]
