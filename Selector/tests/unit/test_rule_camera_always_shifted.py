
from datetime import datetime

from pytest import fixture, mark

from selector.rule import Rule
from selector.rules import CameraAlwaysShiftedRule

from .test_rule_camera_always_blocked import TestRuleCameraAlwaysBlocked


@mark.unit()
class TestRuleCameraAlwaysShifted(TestRuleCameraAlwaysBlocked):
    @property
    def _attribute_name(self) -> str:
        return "interior_camera_health_response_cve"

    @fixture
    def rule(self):
        return CameraAlwaysShiftedRule()

    def test_rule_name(self, rule: Rule):
        assert rule.rule_name == "Camera completely shifted"
