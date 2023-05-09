"""Voxel keypoints mapper"""
from base.voxel.models import KeyPointsMapper
from metadata.consumer.voxel.constants import KEYPOINTS_SORTED


class VoxelKPMapper(KeyPointsMapper):
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
