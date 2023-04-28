from base.voxel.constants import VOXEL_KEYPOINTS_LABELS, VOXEL_SKELETON_LIMBS, POSE_LABEL
import fiftyone as fo
import logging

_logger = logging.getLogger(__name__)


def __set_dataset_skeleton_configuration(dataset: fo.Dataset) -> None:
    dataset.skeletons = {
        POSE_LABEL: fo.KeypointSkeleton(
            labels=VOXEL_KEYPOINTS_LABELS,
            edges=VOXEL_SKELETON_LIMBS,
        )
    }


def create_dataset(dataset_name, tags) -> fo.Dataset:
    """
    Creates a voxel dataset with the given name or loads it if it already exists.

    Args:
        dataset_name: Dataset name to load or create
        tags: Tags to add on dataset creation
    """
    if fo.dataset_exists(dataset_name):
        return fo.load_dataset(dataset_name)
    else:
        dataset = fo.Dataset(dataset_name, persistent=True)
        __set_dataset_skeleton_configuration(dataset)
        if tags is not None:
            dataset.tags = tags
        _logger.info("Voxel dataset [%s] created!", dataset_name)
        return dataset
