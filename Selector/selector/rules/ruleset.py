"""Module that contains the rulesets"""
from typing import Iterator

from selector.rule import Rule
from selector.rules import CameraAlwaysBlockedRule
from selector.rules import CameraAlwaysShiftedRule
from selector.rules import CHCEveryMinute


def ruleset() -> Iterator[Rule]:
    """Default ruleset"""
    yield CameraAlwaysBlockedRule()
    yield CameraAlwaysShiftedRule()
    yield CHCEveryMinute()
