import fiftyone as fo
import logging
from Metadata.metadata.consumer.voxel.metadata_parser import MetadataParser
from Metadata.metadata.consumer.voxel.metadata_artifacts import Frame, DataLoader
from metadata.consumer.voxel.constants import VOXEL_KEYPOINTS_LABELS
from typing import Callable, Optional
from kink import inject

_logger = logging.getLogger(__name__)


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


@inject
class VoxelSnapshotMetadataLoader(DataLoader):
    """
    Class responsible for uploading Frame data to voxel.
    The object shall be created with the correct sample to be updated, and then
    the method "load" can be called to load all the metadata of a single frame.
    Once this object is destroyed the sample is saved to the database.
    """

    def __init__(self,
                 sample: fo.Sample,
                 kp_mapper: Callable[[str],
                                     int],
                 classification_label: str,
                 pose_label: str,
                 bbox_label: str):
        """
        Create a snapshot loader.

        Args:
            sample (fo.Sample): The voxel sample to be updated.
            kp_mapper (Callable[[str], int]): A callback function that is responsible for returning
                the position of a keypoint given it's name. Which is used to map the correct keypoints
                with the pose.

            classification_label (str): The label of the group of classifications.
            pose_label (str): The label for the pose keypoints.
            bbox_label (str): The label for the Bounding boxes.
        """
        self.__sample = sample
        self.__kp_mapper = kp_mapper
        self.__classification_label = classification_label
        self.__pose_label = pose_label
        self.__bbox_label = bbox_label

    def __del__(self):
        """
        Saves the sample when the object goes out of scope.
        """
        self.__sample.save()

    def load(self, frame: Frame):
        """
        Load all the data from a frame.

        Args:
            frames (list[Frame]): A list of frames.

        Raises:
            ValueError: If there is more then one frame in the list of frames.
        """
        self.load_bbox(frame)
        self.load_pose_keypoints(frame)
        self.load_classifications(frame)

    def load_pose_keypoints(self, frame: Frame):
        """
        Load keypoints from a Frame.

        Args:
            frame (Frame): The frame from where to load the keypoints.
        """
        tmp_keypoints_voxel: list[fo.Keypoint] = []

        for person in frame.persons:
            # If there is any keypoint missing it should be filled with None
            tmp_keypoints: list[tuple[Optional[float], Optional[float]]] = [(None, None)] * len(VOXEL_KEYPOINTS_LABELS)
            tmp_confidence: list[Optional[float]] = [None] * len(VOXEL_KEYPOINTS_LABELS)

            for keypoint in person.keypoints:
                keypoint_index: int = self.__kp_mapper(keypoint.name)
                x = to_relative_coord(keypoint.x, frame.width)
                y = to_relative_coord(keypoint.y, frame.height)

                tmp_keypoints[keypoint_index] = (x, y)
                tmp_confidence[keypoint_index] = keypoint.confidence

            voxel_keypoint = fo.Keypoint(
                label=f"Person {len(tmp_keypoints_voxel)}",
                points=tmp_keypoints,
                confidence=tmp_confidence
            )
        tmp_keypoints_voxel.append(voxel_keypoint)
        self.__sample[self.__pose_label] = fo.Keypoints(keypoints=tmp_keypoints_voxel)

    def load_classifications(self, frame: Frame):
        """
        Load classifications from a Frame.

        Args:
            frame (Frame): The frame from where to load the classifications.
        """
        tmp_classifications: list[fo.Classification] = []
        for classification in frame.classifications:
            tmp_classifications.append(fo.Classification(label=classification.name, confidence=classification.value))

        self.__sample[self.__classification_label] = fo.Classifications(classifications=tmp_classifications)

    def load_bbox(self, frame: Frame):
        """
        Load bounding boxes from a Frame.

        Args:
            frame (Frame): The frame from where to load the Bounding Boxes.
        """

        tmp_bbox: list[fo.Detection] = []
        for bbox in frame.bboxes:
            voxel_detection = fo.Detection(
                bounding_box=[
                    to_relative_coord(bbox.x, frame.width),
                    to_relative_coord(bbox.height, frame.height),
                    to_relative_coord(bbox.width, frame.width),
                    to_relative_coord(bbox.height, frame.height)],
                confidence=bbox.confidence)

            tmp_bbox.append(voxel_detection)

        self.__sample[self.__bbox_label] = fo.Detections(detections=tmp_bbox)
