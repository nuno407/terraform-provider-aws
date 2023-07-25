# Used to visualize in Voxel
VOXEL_KEYPOINTS_LABELS = [
    "left_ankle",
    "left_ear",
    "left_elbow",
    "left_eye",
    "left_hip",
    "left_knee",
    "left_shoulder",
    "left_wrist",
    "neck",
    "nose",
    "right_ankle",
    "right_ear",
    "right_elbow",
    "right_eye",
    "right_hip",
    "right_knee",
    "right_shoulder",
    "right_wrist"
]

# Used to correctly import OpenLabel labels from Kognic
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

# Predictions
CLASSIFICATION_LABEL = "PRED_device_classifications"
BBOX_LABEL = "PRED_device_bbox"
POSE_LABEL = "PRED_device_pose"

# Ground truth
GT_POSE_LABEL = "GT_pose"
GT_SEMSEG_LABEL = "GT_semseg"
GT_DETECTIONS_LABEL = "GT_detections"

# Ground truth
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