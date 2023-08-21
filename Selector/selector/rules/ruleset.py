"""Module that contains the rulesets"""

from selector.rule import Rule
from selector.rules import CameraAlwaysBlockedRule
from selector.rules import CameraAlwaysShiftedRule
from selector.rules import CHCEveryMinute
from selector.rules import HighPersonCountVarianceRule
from selector.rules import BDDEvent


def ruleset() -> set[Rule]:
    """Default ruleset"""
    return {
        CameraAlwaysBlockedRule(),
        CameraAlwaysShiftedRule(),
        CHCEveryMinute(),
        HighPersonCountVarianceRule(),
        BDDEvent()
    }
