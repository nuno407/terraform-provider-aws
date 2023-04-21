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


VOXEL_SKELETON_LIMBS = [[9, 3],
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

# A map to specify the position of each keypoint
KEYPOINTS_SORTED = {
    "LeftAnkle": 0,
    "LeftEar": 1,
    "LeftElbow": 2,
    "LeftEye": 3,
    "LeftHip": 4,
    "LeftKnee": 5,
    "LeftShoulder": 6,
    "LeftWrist": 7,
    "Neck": 8,
    "Nose": 9,
    "RightAnkle": 10,
    "RightEar": 11,
    "RightElbow": 12,
    "RightEye": 13,
    "RightHip": 14,
    "RightKnee": 15,
    "RightShoulder": 16,
    "RightWrist": 17
}

CLASSIFICATION_LABEL = "PRED_device_classifications"
BBOX_LABEL = "PRED_device_bbox"
POSE_LABEL = "PRED_device_pose"
