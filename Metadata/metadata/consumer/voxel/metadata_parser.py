from base.model.metadata_artifacts import Frame, KeyPoint, Person, BoundingBox, Classification
import logging
_logger = logging.getLogger(__name__)


class MetadataParser:
    """
    Class responsible for parsing and converting a frame from the metadata to the voxel format currently difined here (TODO: place specification doc link).
    The function "parse" that will parse the metadata file and return a list of frames.
    """

    @staticmethod
    def parse(metadata_json: dict) -> list[Frame]:
        """
        Parses the entire metadatafull file.
        TODO: Add timestamps to the Frame if exists

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

        _logger.info(
            "Metadata has been parsed sucessfully, %d frames parsed", len(frame_list))

        return frame_list

    @staticmethod
    def parse_person_details(person_details: dict, person_id: int) -> Person:
        """
        Parses the person details and stores the processed person pose keypoints in a Person object.
        The person details is a dictionary with a field called "KeyPoint" which contains a list of dicts with the following fields:
        - Conf
        - Name
        - X
        - Y

        Keypoints with a confidence of 0 or that are not valid or outside of frame will be skipped.

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

            if confidence > 0.01:
                kp = KeyPoint(x=keypoint_x, y=keypoint_y,
                              confidence=confidence, name=keypoint_name)
                tmp_keypoints.append(kp)

        return Person(keypoints=tmp_keypoints, name=f"Person {person_id}")

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
        object_id = obj["id"]
        confidence = float(obj["confidence"])

        return BoundingBox(
            x=person_box_x,
            y=person_box_y,
            width=person_box_width,
            height=person_box_height,
            confidence=confidence,
            name=object_id)

    @staticmethod
    def parse_float_attributes(float_list: list) -> list[Classification]:
        """
        Parses a list of floats predictions.
        The float_list shall be a list of dictionaries with the following fields:
        - name
        - value

        Remark: all the fields are expected to be in string format.

        Args:
            float_list (list[dict]): A list of floatAttributes from the metadata file.

        Return:
            list[Classification]: A list of Classifications with absolute coordinates.
        """

        tmp_list_classifications = []

        for float_dict in float_list:
            name = float_dict["name"]
            value = float(float_dict["value"])
            tmp_list_classifications.append(
                Classification(name=name, value=value))

        return tmp_list_classifications

    @staticmethod
    def parse_bool_attributes(bool_list: list) -> list[Classification]:
        """
        Parses a list of boolean predictions and converts them to a float, either 1.0 or 0.0.
        The float_list shall be a list of dictionaries with the fowlloing fields:
        - name
        - value

        Remark: All the fields are expected in string format (including the float value)

        Args:
            float_list (list[dict]): A list of floatAttributes from the metadata file.

        Raises:
            ValureError: If one of the attributes is neither false nor true.

        Return:
            list[Classification]: A list of Classifications with absolute coordinates.
        """

        tmp_list_classifications = []

        for bool_dict in bool_list:
            name = bool_dict["name"]
            value_str_low: str = bool_dict["value"].lower()

            if "true" == value_str_low:
                tmp_list_classifications.append(
                    Classification(name=name, value=1.0))
            elif "false" == value_str_low:
                tmp_list_classifications.append(
                    Classification(name=name, value=0.0))
            else:
                raise ValueError(
                    f"Attribute {name} with value ({value_str_low}) cannot be converted to boolean")

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
            Frame: A frame object with absolute coordinates.
        """
        detection_boxes: list[BoundingBox] = []
        persons: list[Person] = []
        classifications: list[Classification] = []

        for obj in frame.get("objectlist", {}):
            if "box" in obj:
                box = MetadataParser.parse_detection_box(obj)
                if box.confidence > 0.01:
                    detection_boxes.append(box)

            if "personDetail" in obj:
                persons.append(MetadataParser.parse_person_details(
                    obj["personDetail"], len(persons)))

            if "floatAttributes" in obj:
                classifications.extend(
                    MetadataParser.parse_float_attributes(obj["floatAttributes"]))

            if "boolAttributes" in obj:
                classifications.extend(
                    MetadataParser.parse_bool_attributes(obj["boolAttributes"]))

        return Frame(
            persons=persons,
            bboxes=detection_boxes,
            classifications=classifications,
            width=width,
            height=height)
