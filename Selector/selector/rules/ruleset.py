"""Module that contains the rulesets"""

from selector.rule import Rule
from selector.rules import AudioHealth
from selector.rules import AudioSignal
from selector.rules import CameraAlwaysBlockedRule
from selector.rules import CameraAlwaysShiftedRule
from selector.rules import CHCEveryMinute
from selector.rules import HighPersonCountVarianceRule
from selector.rules import BDDEvent
from selector.rules import CollisionEvent


def ruleset() -> set[Rule]:
    """Default ruleset"""
    return {
        AudioHealth("interior_camera_health_response_audio_blocked", "Audio blocked", "1.0.0"),
        AudioHealth("interior_camera_health_response_audio_distorted", "Audio distorted", "1.0.0"),
        AudioSignal(),
        CameraAlwaysBlockedRule(),
        CameraAlwaysShiftedRule(),
        CHCEveryMinute(),
        HighPersonCountVarianceRule(),
        BDDEvent(),
        CollisionEvent()
    }
