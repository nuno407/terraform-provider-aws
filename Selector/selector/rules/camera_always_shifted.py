"""Rule for camera always shifted"""
from selector.rules.camera_always_blocked import CameraAlwaysBlockedRule


class CameraAlwaysShiftedRule(CameraAlwaysBlockedRule):
    """Rule that uploads training data if interior_camera_health_response_cve is always bigger then 1.0"""
    @property
    def rule_name(self) -> str:
        return "Camera completely shifted"

    @property
    def _attribute_name(self) -> str:
        return "interior_camera_health_response_cve"
