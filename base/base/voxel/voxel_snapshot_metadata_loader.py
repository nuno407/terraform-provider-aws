import fiftyone as fo
from base.model.metadata_artifacts import Frame, DataLoader
from base.voxel.constants import VOXEL_KEYPOINTS_LABELS, CLASSIFICATION_LABEL, BBOX_LABEL, POSE_LABEL
from typing import Optional
from base.voxel.models import KeyPointsMapper
from kink import inject
import logging
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
    Before using attempting to load any data, the function "set_sample" needs to be called in order
    set a sample to be updated.

    TODO: Do value range checks (eg: if bounding box is out of frame)
    """

    SAMPLE_NOT_SET_ERROR = "Error while loading bounding boxes 'set_sample' needs to be called first"

    def __init__(self,
                 kp_mapper: KeyPointsMapper,
                 classification_label: str = CLASSIFICATION_LABEL,
                 pose_label: str = BBOX_LABEL,
                 bbox_label: str = POSE_LABEL):
        """
        Create a snapshot loader.

        Args:
            sample (fo.Sample): The voxel sample to be updated. The caller is responsible for saving.
            kp_mapper (KeyPointsMapper): Responsible for returning
                the position of a keypoint given it's name. Which is used to map the correct keypoints
                with the pose.

            classification_label (str): The label of the group of classifications.
            pose_label (str): The label for the pose keypoints.
            bbox_label (str): The label for the Bounding boxes.
        """
        self.__sample: Optional[fo.Sample]
        self.__kp_mapper = kp_mapper
        self.__classification_label = classification_label
        self.__pose_label = pose_label
        self.__bbox_label = bbox_label

    def set_sample(self, sample: fo.Sample):
        """
        Sets a sample to be updated.

        IMPORTANT REMARK: The caller is responsible for saving the sample!
        Args:
            frame (fo.Sample): The sample to be updated.
        """
        self.__sample = sample

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

        _logger.info(
            "Frame metadata has been uploaded to Voxel")

    def load_pose_keypoints(self, frame: Frame):
        """
        Load keypoints from a Frame.

        REMARKS: The oclusion is not loaded yet!

        Args:
            frame (Frame): The frame from where to load the keypoints.
        """
        if self.__sample is None:
            raise ValueError(
                VoxelSnapshotMetadataLoader.SAMPLE_NOT_SET_ERROR)

        if not len(frame.persons):
            return

        tmp_keypoints_voxel: list[fo.Keypoint] = []

        for person in frame.persons:
            # If there is any keypoint missing it should be filled with None
            tmp_keypoints: list[tuple[Optional[float], Optional[float]]] = [
                (None, None)] * len(VOXEL_KEYPOINTS_LABELS)
            tmp_confidence: list[Optional[float]] = [
                0.0] * len(VOXEL_KEYPOINTS_LABELS)

            for keypoint in person.keypoints:
                keypoint_index: int = self.__kp_mapper.get_keypoint_index(
                    keypoint.name)
                x = to_relative_coord(keypoint.x, frame.width)
                y = to_relative_coord(keypoint.y, frame.height)

                tmp_keypoints[keypoint_index] = (x, y)
                tmp_confidence[keypoint_index] = keypoint.confidence

            voxel_keypoint = fo.Keypoint(
                label=person.name,
                points=tmp_keypoints,
                confidence=tmp_confidence
            )
            tmp_keypoints_voxel.append(voxel_keypoint)
        self.__sample[self.__pose_label] = fo.Keypoints(
            keypoints=tmp_keypoints_voxel)

    def load_classifications(self, frame: Frame):
        """
        Load classifications from a Frame.

        Args:
            frame (Frame): The frame from where to load the classifications.
        """
        if self.__sample is None:
            raise ValueError(
                VoxelSnapshotMetadataLoader.SAMPLE_NOT_SET_ERROR)

        if not len(frame.classifications):
            return

        tmp_classifications: list[fo.Classification] = []
        for classification in frame.classifications:
            tmp_classifications.append(fo.Classification(
                label=classification.name, confidence=classification.value))

        self.__sample[self.__classification_label] = fo.Classifications(
            classifications=tmp_classifications)

    def load_bbox(self, frame: Frame):
        """
        Load bounding boxes from a Frame.

        Args:
            frame (Frame): The frame from where to load the Bounding Boxes.
        """
        if self.__sample is None:
            raise ValueError(
                VoxelSnapshotMetadataLoader.SAMPLE_NOT_SET_ERROR)

        if not len(frame.bboxes):
            return

        tmp_bbox: list[fo.Detection] = []
        for bbox in frame.bboxes:
            voxel_detection = fo.Detection(
                bounding_box=[
                    to_relative_coord(bbox.x, frame.width),
                    to_relative_coord(bbox.height, frame.height),
                    to_relative_coord(bbox.width, frame.width),
                    to_relative_coord(bbox.height, frame.height)],
                confidence=bbox.confidence,
                label=bbox.name)

            tmp_bbox.append(voxel_detection)

        self.__sample[self.__bbox_label] = fo.Detections(detections=tmp_bbox)
