"""Additional methods for voxel"""
from kink import inject
from base.model.metadata.media_metadata import MediaKeypointName
from base.voxel.constants import VoxelKeyPointLabel, VOXEL_KEYPOINTS_LABELS
from artifact_api.exceptions import VoxelProcessingException


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

# pylint: disable=too-few-public-methods


@inject
class VoxelKPMapper:
    """
    Voxel Keypoints mapper
    Used for setting up bones in Voxel.

    It uses the index of the keypoint in the list of keypoints to determine the position of the keypoint.
    """

    def __init__(self):
        kp_sorted = {
            MediaKeypointName.LEFT_ANKLE: VoxelKeyPointLabel.LEFT_ANKLE,
            MediaKeypointName.LEFT_EAR: VoxelKeyPointLabel.LEFT_EAR,
            MediaKeypointName.LEFT_ELBOW: VoxelKeyPointLabel.LEFT_ELBOW,
            MediaKeypointName.LEFT_EYE: VoxelKeyPointLabel.LEFT_EYE,
            MediaKeypointName.LEFT_HIP: VoxelKeyPointLabel.LEFT_HIP,
            MediaKeypointName.LEFT_KNEE: VoxelKeyPointLabel.LEFT_KNEE,
            MediaKeypointName.LEFT_SHOULDER: VoxelKeyPointLabel.LEFT_SHOULDER,
            MediaKeypointName.LEFT_WRIST: VoxelKeyPointLabel.LEFT_WRIST,
            MediaKeypointName.NECK: VoxelKeyPointLabel.NECK,
            MediaKeypointName.NOSE: VoxelKeyPointLabel.NOSE,
            MediaKeypointName.RIGHT_ANKLE: VoxelKeyPointLabel.RIGHT_ANKLE,
            MediaKeypointName.RIGHT_EAR: VoxelKeyPointLabel.RIGHT_EAR,
            MediaKeypointName.RIGHT_ELBOW: VoxelKeyPointLabel.RIGHT_ELBOW,
            MediaKeypointName.RIGHT_EYE: VoxelKeyPointLabel.RIGHT_EYE,
            MediaKeypointName.RIGHT_HIP: VoxelKeyPointLabel.RIGHT_HIP,
            MediaKeypointName.RIGHT_KNEE: VoxelKeyPointLabel.RIGHT_KNEE,
            MediaKeypointName.RIGHT_SHOULDER: VoxelKeyPointLabel.RIGHT_SHOULDER,
            MediaKeypointName.RIGHT_WRIST: VoxelKeyPointLabel.RIGHT_WRIST
        }
        self.__kp_index = {m_kp: VOXEL_KEYPOINTS_LABELS.index(v_kp) for m_kp, v_kp in kp_sorted.items()}

    def get_keypoint_index(self, name: MediaKeypointName) -> int:
        """
        Shall return a desirable position for a keypoint name to be used on the voxel loader.

        Args:
            name (str): Name of the keypoint

        Returns:
            int: The index for the keypoint
        """

        if name not in self.__kp_index:
            raise VoxelProcessingException(f"Keypoint {name} is not supported")

        return self.__kp_index[name]
