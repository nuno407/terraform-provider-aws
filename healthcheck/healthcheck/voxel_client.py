# pylint: disable=R0903:too-few-public-methods
# type: ignore
"""Database interface module."""

from enum import Enum
from typing import Protocol

from kink import inject


class VoxelDataset(Enum):
    """Voxel video dataset"""
    VIDEOS: str = "Debug_Lync"
    SNAPSHOTS: str = "Debug_Lync_snapshots"


class VoxelEntriesGetter(Protocol):
    """Interface to abstract voxel client get operation.

    This interface enables us to easily mock the client's behavior in the tests.
    """

    def get_num_entries(self, file_path: str, dataset: VoxelDataset) -> int:
        """get voxel inserted entries based on filepath and dataset name

        Args:
            file_path (str): path to file in voxel DB
            dataset (VoxelDataset): voxel data set name

        Returns:
            int: entries count
        """


@inject(alias=VoxelEntriesGetter)
class VoxelClient():
    """Mongo DB client abstraction interface."""

    def get_num_entries(self, file_path: str, dataset: VoxelDataset) -> int:
        """
        Return the number of entries in a specific dataset.

        Args:
            file_path (str): Path to the entry.
            dataset (str): Dataset of the entry

        Returns:
            int: Number of entries found
        """
        import fiftyone as fo  # pylint: disable=C0415

        if not fo.dataset_exists(dataset.value):
            raise ValueError(f"Voxel dataset {dataset.value} does not exist")

        voxel_dataset = fo.load_dataset(dataset.value)
        view = voxel_dataset.select_by("filepath", file_path)
        return view.count()
