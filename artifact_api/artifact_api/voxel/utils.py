from typing import Optional
from dataclasses import dataclass
import fiftyone as fo
from kink import inject

def to_relative_coord(abs_val: int, max_val: int) -> float:
    """
    Converts an absolute value to a relative one.

    Args:
        abs_val (int): The absolute value.
        max_val (int): The maximum value.

    Returns:
        float: The relative value in a range [0,1]
    """
    return abs_val / max_val

KEYPOINTS_SORTED = {
    "LeftAnkle": 0,
    "LeftEar": 1,
    "LeftElbow": 2,
    "LeftEye": 3,
    "LeftHip": 4,
    "LeftKnee": 5,
    "LeftShoulder": 6,
    "LeftWrist": 7,
    "Neck": 8,
    "Nose": 9,
    "RightAnkle": 10,
    "RightEar": 11,
    "RightElbow": 12,
    "RightEye": 13,
    "RightHip": 14,
    "RightKnee": 15,
    "RightShoulder": 16,
    "RightWrist": 17
}

@inject
class VoxelKPMapper:
    """
    Voxel Keypoints mapper
    Used for setting up bones in Voxel.

    """

    def __init__(self):
        self.__kp_sorted = KEYPOINTS_SORTED

    def get_keypoint_index(self, name: str) -> int:
        """
        Shall return a desirable position for a keypoint name to be used on the voxel loader.

        Args:
            name (str): Name of the keypoint

        Returns:
            int: The index for the keypoint
        """

        return self.__kp_sorted[name]