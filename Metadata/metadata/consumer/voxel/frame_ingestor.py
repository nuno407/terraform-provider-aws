from typing import Any
import fiftyone as fo


class VoxelFrameParser:
    """
    Class responsible for parsing and converting the metadafull to the voxel format currently difined here (TODO: place specification doc link).
    """

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

    def __init__(self, frame_width: int, frame_height: int):
        """
        Args:
            frame_data (dict[str, Any]): One frame from the metadata file.
            frame_width (int): The width of the frame.
            frame_height (int): The height of the frame.
        """
        self.__frame_width = frame_width
        self.__frame_height = frame_height
        self.__bboxes: list[fo.Detection] = []
        self.__key_points: list[fo.Keypoint] = []
        self.__classifications: list[fo.Classification] = []

    def get_keypoints(self) -> fo.Keypoints:
        """
        Return the keypoints ready to be loaded into voxel.

        Returns:
            fo.Keypoints: Pose key points.
        """
        return fo.Keypoints(keypoints=self.__key_points)

    def get_bouding_boxes(self) -> fo.Detections:
        """
        Return the bounding boxes for every object in the metdata ready to be loaded into voxel.

        Returns:
            fo.Detections: Bounding boxes.
        """
        return fo.Detections(detections=self.__bboxes)

    def get_classifications(self) -> fo.Classifications:
        """
        Return the classifications for every object in the metdata ready to be loaded into voxel.

        Returns:
            fo.Classifications: Classificaitons.
        """
        return fo.Classifications(classifications=self.__classifications)

    def parse(self, frame_data: dict):
        """
        Parses the metadata and caches the processed information.
        """
        self.__bboxes.clear()
        self.__key_points.clear()
        for obj in frame_data.get("objectlist", {}):
            self.__parse_object(obj)

    def __str_relateive_width(self, absolute_width: str) -> float:
        """
        Converts and parses an absolute width to a relative width (0-1) based on the frame width.

        Args:
            absolute_width (str): The absolute width in an integer format.

        Returns:
            float: The converted value.
        """
        rel_val = int(absolute_width) / self.__frame_width
        rel_val = min(1, rel_val)
        return rel_val

    def __str_relateive_height(self, absolute_height: str) -> float:
        """
        Converts and parses an absolute height to a relative height (0-1) based on the frame height.

        Args:
            absolute_height (str): The absolute height in an integer format.

        Returns:
            float: The converted value.
        """
        rel_val = int(absolute_height) / self.__frame_height
        rel_val = min(1, rel_val)
        return rel_val

    def __parse_person_details(self, person_details: dict):
        """
        Parses the person details and stores the processed person pose keypoints in self.__key_points.
        The person details is a dictionary with a field called "KeyPoint" which contains a list of dicts with the following fields:
        - Conf
        - Name
        - OutOfFrame
        - Valid
        - X
        - Y

        Args:
            person_details (dict[str, Any]): Part of the metadata data that is contained inside "personDetail".
        """
        tmp_keypoints: list[tuple[float, float]] = [(None, None)] * len(self.KEYPOINTS_SORTED)
        tmp_confidence: list[float] = [None] * len(self.KEYPOINTS_SORTED)

        for keypoint in person_details["KeyPoint"]:
            confidence = float(keypoint["Conf"])
            keypoint_name = keypoint["Name"]
            keypoint_out_frame = bool(int(keypoint["OutOfFrame"]))
            keypoint_valid = bool(int(keypoint["Valid"]))
            keypoint_index = self.KEYPOINTS_SORTED[keypoint_name]

            # If the confidence is null the the point does not exist
            if confidence > 0.01 and not keypoint_out_frame and keypoint_valid:
                keypoint_x = self.__str_relateive_width(keypoint["X"])
                keypoint_y = self.__str_relateive_height(keypoint["Y"])

                tmp_keypoints[keypoint_index] = (keypoint_x, keypoint_y)

            tmp_confidence[keypoint_index] = confidence

        voxel_keypoint = fo.Keypoint(
            label=f"Person {len(self.__key_points)}",
            points=tmp_keypoints,
            confidence=tmp_confidence
        )
        self.__key_points.append(voxel_keypoint)

    def __parse_detection_box(self, obj: dict):
        """
        Parses a detection box and stores it in self.__bboxes.
        The obj is a dictionary that should contain the following fields:
        - box
        - confidence
        - id
        -- x
        -- y
        --height
        -- width

        Args:
            obj (dict[str, Any]): A dictionary with metadata for a single box.
        """
        person_box_width = self.__str_relateive_width(obj["box"]["width"])
        person_box_height = self.__str_relateive_height(obj["box"]["height"])
        person_box_x = self.__str_relateive_width(obj["box"]["x"])
        person_box_y = self.__str_relateive_height(obj["box"]["y"])
        confidence = float(obj["confidence"])
        if confidence > 0.01:
            voxel_detection = fo.Detection(
                bounding_box=[
                    person_box_x,
                    person_box_y,
                    person_box_width,
                    person_box_height],
                index=int(
                    obj["id"]),
                confidence=confidence)

            self.__bboxes.append(voxel_detection)

    def __parse_float_attributes(self, float_list: list):
        """
        Parses a list of floats predictions and loads them into classification.
        The float_list shall be a list of dictionaries with the fowlloing fields:
        - name
        - value

        Args:
            float_list (list[dict]): A list of floatAttributes from the metadata file.
        """

        for float_dict in float_list:
            name = float_dict["name"]
            value = float(float_dict["value"])
            self.__classifications.append(fo.Classification(label=name, confidence=value))

    def __parse_object(self, obj: dict):
        """
        Parses an object form the metadata, currently it only parses persons poses and objects with bouding boxes.

        Args:
            obj (dict[str, Any]): A dictionary with metadata for a single object.
        """
        if "box" in obj:
            self.__parse_detection_box(obj)

        if "personDetail" in obj:
            self.__parse_person_details(obj["personDetail"])

        if "floatingAttributes" in obj:
            self.__parse_float_attributes(obj["floatAttributes"])
