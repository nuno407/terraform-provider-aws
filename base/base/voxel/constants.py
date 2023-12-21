from enum import Enum
from typing import Final


class VoxelKeyPointLabel(str, Enum):
    LEFT_ANKLE = "left_ankle"
    LEFT_EAR = "left_ear"
    LEFT_ELBOW = "left_elbow"
    LEFT_EYE = "left_eye"
    LEFT_HIP = "left_hip"
    LEFT_KNEE = "left_knee"
    LEFT_SHOULDER = "left_shoulder"
    LEFT_WRIST = "left_wrist"
    NECK = "neck"
    NOSE = "nose"
    RIGHT_ANKLE = "right_ankle"
    RIGHT_EAR = "right_ear"
    RIGHT_ELBOW = "right_elbow"
    RIGHT_EYE = "right_eye"
    RIGHT_HIP = "right_hip"
    RIGHT_KNEE = "right_knee"
    RIGHT_SHOULDER = "right_shoulder"
    RIGHT_WRIST = "right_wrist"


# Used to visualize in Voxel
# This list cannot be changed at runtime
VOXEL_KEYPOINTS_LABELS: Final[list[VoxelKeyPointLabel]] = [
    VoxelKeyPointLabel.LEFT_ANKLE,
    VoxelKeyPointLabel.LEFT_EAR,
    VoxelKeyPointLabel.LEFT_ELBOW,
    VoxelKeyPointLabel.LEFT_EYE,
    VoxelKeyPointLabel.LEFT_HIP,
    VoxelKeyPointLabel.LEFT_KNEE,
    VoxelKeyPointLabel.LEFT_SHOULDER,
    VoxelKeyPointLabel.LEFT_WRIST,
    VoxelKeyPointLabel.NECK,
    VoxelKeyPointLabel.NOSE,
    VoxelKeyPointLabel.RIGHT_ANKLE,
    VoxelKeyPointLabel.RIGHT_EAR,
    VoxelKeyPointLabel.RIGHT_ELBOW,
    VoxelKeyPointLabel.RIGHT_EYE,
    VoxelKeyPointLabel.RIGHT_HIP,
    VoxelKeyPointLabel.RIGHT_KNEE,
    VoxelKeyPointLabel.RIGHT_SHOULDER,
    VoxelKeyPointLabel.RIGHT_WRIST
]

VOXEL_SKELETON_LIMBS = [
    [9, 3],
    [3, 1],
    [9, 13],
    [13, 11],
    [8, 9],
    [6, 8],
    [2, 6],
    [7, 2],
    [16, 8],
    [12, 16],
    [17, 12],
    [4, 8],
    [5, 4],
    [0, 5],
    [14, 8],
    [15, 14],
    [10, 15]
]

# Used to correctly import OpenLabel labels from Kognic
# It is not used to visualize in Voxel
OPENLABEL_KEYPOINTS_LABELS = [
    "LeftAnkle",
    "LeftEar",
    "LeftElbow",
    "LeftEye",
    "LeftHip",
    "LeftKnee",
    "LeftShoulder",
    "LeftWrist",
    "neck",
    "nose",
    "RightAnkle",
    "RightEar",
    "RightElbow",
    "RightEye",
    "RightHip",
    "RightKnee",
    "RightShoulder",
    "RightWrist"
]

# Predictions
CLASSIFICATION_LABEL = "PRED_device_classifications"
BBOX_LABEL = "PRED_device_bbox"
POSE_LABEL = "PRED_device_pose"

# Ground truth
GT_POSE_LABEL = "GT_pose"
GT_SEMSEG_LABEL = "GT_semseg"
GT_DETECTIONS_LABEL = "GT_detections"

# Inference
INFERENCE_POSE = "PRED_inference_pose"
INFERENCE_SEMSEG = "PRED_inference_semseg"
INFERENCE_DETECTIONS = "PRED_inference_detections"

OPENLABEL_LABEL_MAPPING = {
    "keypoints": GT_POSE_LABEL,
    "segmentations": GT_SEMSEG_LABEL,
    "detections": GT_DETECTIONS_LABEL,
}


OPENLABEL_LABEL_MAPPING_INFERENCE = {
    "keypoints": INFERENCE_POSE,
    "segmentations": INFERENCE_SEMSEG,
    "detections": INFERENCE_DETECTIONS,
}
