from metadata.consumer.voxel.metadata_artifacts import Frame, KeyPoint, Person, BoundingBox, Classification


class MetadataParser:
    """
    Class responsible for parsing and converting a frame from the metadata to the voxel format currently difined here (TODO: place specification doc link).
    The function "parse" that will parse the metadata file and return a list of frames.
    """

    @staticmethod
    def parse(metadata_json: dict) -> list[Frame]:
        """
        Parses the entire metadatafull file.

        Args:
            frame_data (dict): The frame data, this should be just one frame that is contained inside the metadata "frame" array.
        returns:
            list[Frame]: Returns a list of frames.
        """
        width = int(metadata_json["resolution"]["width"])
        height = int(metadata_json["resolution"]["height"])
        frame_list: list[Frame] = []
        for obj in metadata_json.get("frame", []):
            frame_list.append(MetadataParser.parse_frame(obj, width, height))

        return frame_list

    @staticmethod
    def parse_person_details(person_details: dict) -> Person:
        """
        Parses the person details and stores the processed person pose keypoints in a Person object.
        The person details is a dictionary with a field called "KeyPoint" which contains a list of dicts with the following fields:
        - Conf
        - Name
        - OutOfFrame
        - Valid
        - X
        - Y

        Args:
            person_details (dict[str, Any]): Part of the metadata data that is contained inside "personDetail".

        Returns:
            Person: A person object containing the keypoints.
        """
        tmp_keypoints: list[KeyPoint] = []

        for keypoint in person_details["KeyPoint"]:
            confidence = float(keypoint["Conf"])
            keypoint_x = int(keypoint["X"])
            keypoint_y = int(keypoint["Y"])
            keypoint_name = keypoint["Name"]
            keypoint_out_frame = bool(int(keypoint["OutOfFrame"]))
            keypoint_valid = bool(int(keypoint["Valid"]))

            if confidence > 0.01 and not keypoint_out_frame and keypoint_valid:
                kp = KeyPoint(keypoint_x, keypoint_y, confidence, keypoint_name)
                tmp_keypoints.append(kp)

        return Person(tmp_keypoints)

    @staticmethod
    def parse_detection_box(obj: dict) -> BoundingBox:
        """
        Parses the data into a BoundingBox.
        The obj is a dictionary that should contain the following fields:
        - confidence
        - id
        - box
        -- x
        -- y
        -- height
        -- width

        Args:
            obj (dict[str, Any]): A dictionary with metadata for a single box.

        Return:
            BoundingBox: A BoundingBox object with absolute coordinates.
        """
        person_box_width = int(obj["box"]["width"])
        person_box_height = int(obj["box"]["height"])
        person_box_x = int(obj["box"]["x"])
        person_box_y = int(obj["box"]["y"])
        confidence = float(obj["confidence"])

        return BoundingBox(person_box_x, person_box_y, person_box_width, person_box_height, confidence)

    @staticmethod
    def parse_float_attributes(float_list: list) -> list[Classification]:
        """
        Parses a list of floats predictions and loads them into classification.
        The float_list shall be a list of dictionaries with the fowlloing fields:
        - name
        - value

        Args:
            float_list (list[dict]): A list of floatAttributes from the metadata file.

        Return:
            list[Classification]: A list of Classifications with absolute coordinates.
        """

        tmp_list_classifications = []

        for float_dict in float_list:
            name = float_dict["name"]
            value = float(float_dict["value"])
            tmp_list_classifications.append(Classification(name, value))

        return tmp_list_classifications

    @staticmethod
    def parse_frame(frame: dict, width: int, height: int) -> Frame:
        """
        Parses an object form the metadata, currently it only parses persons poses, bouding boxes and box attributes.

        Args:
            frame_data (dict): The frame data, this should be just one frame that is contained inside the metadata "frame" array.
            width (int): Width of the frame.
            height (int): Height of the frame.

        Returns:
            Frame: A frame bject with absolute coordinates.
        """
        detection_boxes: list[BoundingBox] = []
        persons: list[Person] = []
        classifications: list[Classification] = []

        for obj in frame.get("objectlist", {}):
            if "box" in obj:
                detection_boxes.append(MetadataParser.parse_detection_box(obj))

            if "personDetail" in obj:
                persons.append(MetadataParser.parse_person_details(obj["personDetail"]))

            if "floatingAttributes" in obj:
                classifications.extend(MetadataParser.parse_float_attributes(obj["floatAttributes"]))

        return Frame(persons, detection_boxes, classifications, width, height)
