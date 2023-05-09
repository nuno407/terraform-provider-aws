from abc import ABC, abstractmethod


class KeyPointsMapper(ABC):
    """
    An interface to transform a keypoint name into an index
    """
    @abstractmethod
    def get_keypoint_index(self, name: str) -> int:
        """
        Shall return a desirable position for a keypoint name to be used on the voxel loader.

        Args:
            name (str): Name of the keypoint

        Raises:
            NotImplementedError: Needs to be implemented by the superclass

        Returns:
            int: The index for the keypoint
        """
        raise NotImplementedError
