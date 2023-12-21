"""Voxel class reponsible for transforming a snapshot metadata into the respective voxel fields"""
from dataclasses import dataclass
from typing import Optional
import logging
import fiftyone as fo
from kink import inject

from base.model.metadata.media_metadata import MediaFrame, MediaMetadata, Resolution
from base.voxel.constants import VOXEL_KEYPOINTS_LABELS
from artifact_api.exceptions import VoxelSnapshotMetadataError
from artifact_api.voxel.utils import to_relative_coord, VoxelKPMapper

_logger = logging.getLogger(__name__)


@dataclass
class VoxelMetadataFrameFields:
    """Voxel frame fields"""
    keypoints: fo.Keypoints
    classifications: fo.Classifications

# pylint: disable=too-few-public-methods


@inject
class VoxelMetadataTransformer:
    """
    Class responsible transforming a snapshot metadata into the respective voxel fields.
    """

    def __init__(self,
                 kp_mapper: VoxelKPMapper):
        """
        Cosntructor.

        Args:
            kp_mapper (KeyPointsMapper): Responsible for returning
                the position of a keypoint given it's name. Which is used to map the correct keypoints
                with the pose.
        """
        self.__kp_mapper = kp_mapper

    def transform_snapshot_metadata_to_voxel(self, metadata: MediaMetadata) -> VoxelMetadataFrameFields:
        """
        Load all the data from a snapshot.

        Args:
            metadata (MediaMetadata): The metadata to be loaded.

        Raises:
            VoxelSnapshotMetadataError: If there is more then one frame in the list of frames.
        """

        if len(metadata.frames) < 1:
            return VoxelMetadataFrameFields(None, None)
        if len(metadata.frames) > 1:
            raise VoxelSnapshotMetadataError(
                "There should be only one frame in the list of frames")

        keypoints = self.get_pose_keypoints(metadata.frames[0], metadata.resolution)
        classifications = self.get_classifications(metadata.frames[0])

        _logger.info(
            "%d keypoints and %d classifications have been converted to voxel format",
            len(keypoints),
            len(classifications))

        return VoxelMetadataFrameFields(keypoints, classifications)

    def get_pose_keypoints(self, frame: MediaFrame, resolution: Resolution) -> fo.Keypoints:
        """
        Load keypoints from a Frame.

        Args:
            frame (MediaFrame): The frame from where to load the keypoints.
            resolution (Resolution): The resolution of the frame.

        Returns:
            fo.Keypoints: The keypoints in voxel format.
        """

        tmp_keypoints_voxel: list[fo.Keypoint] = []

        for i, person in enumerate(frame.object_list.person_details):
            # If there is any keypoint missing it should be filled with None
            tmp_keypoints: list[tuple[Optional[float], Optional[float]]] = [
                (None, None)] * len(VOXEL_KEYPOINTS_LABELS)
            tmp_confidence: list[Optional[float]] = [
                0.0] * len(VOXEL_KEYPOINTS_LABELS)

            for keypoint in person.keypoints:

                # Ensures that keypoints with confidence 0 are not loaded
                if keypoint.conf < 0.000001:
                    continue

                keypoint_index: int = self.__kp_mapper.get_keypoint_index(
                    keypoint.name)
                x_coord = to_relative_coord(keypoint.x, resolution.width)
                y_coord = to_relative_coord(keypoint.y, resolution.height)

                tmp_keypoints[keypoint_index] = (x_coord, y_coord)
                tmp_confidence[keypoint_index] = keypoint.conf

            voxel_keypoint = fo.Keypoint(
                label=f"Person {i}",
                points=tmp_keypoints,
                confidence=tmp_confidence
            )
            tmp_keypoints_voxel.append(voxel_keypoint)
        return fo.Keypoints(keypoints=tmp_keypoints_voxel)

    def get_classifications(self, frame: MediaFrame) -> fo.Classifications:
        """
        Get classifications from a Frame in voxel format.

        Args:
            frame (MediaFrame): The frame from where to load the classifications.

        Returns:
            fo.Classifications: The classifications in voxel format.
        """

        tmp_classifications: list[fo.Classification] = []
        for classification in frame.get_numeric_classifications():
            tmp_classifications.append(fo.Classification(
                label=classification.name, confidence=classification.value))

        return fo.Classifications(classifications=tmp_classifications)
