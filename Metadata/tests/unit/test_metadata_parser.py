import json
import os
from unittest.mock import ANY, Mock, patch

import pytest
from metadata.consumer.voxel.metadata_artifacts import (BoundingBox,
                                                        Classification, Frame,
                                                        Person)
from metadata.consumer.voxel.metadata_parser import MetadataParser

CURRENT_LOCATION = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

METADATA_LOCATION = os.path.join(CURRENT_LOCATION, "test_data", "metadata_data")


def load_json(file_name: str) -> dict:
    """
    Loads a local json file

    Args:
        file_name (str): _description_

    Returns:
        dict: _description_
    """
    file_path = os.path.join(METADATA_LOCATION, file_name)
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_frame_from_json(file_name: str) -> Frame:
    """
    Load the first frame from a json file

    Args:
        file_name (str): _description_

    Returns:
        dict: _description_
    """

    metadata_format = load_json(file_name)
    return MetadataParser.parse(metadata_format)[0]


def load_frame_pydantic(file_name: str) -> Frame:
    """
    Load Frame pydantic model

    Args:
        file_name (str): _description_

    Returns:
        Frame: _description_
    """
    metadata_format = load_json(file_name)
    return Frame(**metadata_format)


@pytest.mark.unit
class TestMetadataParser:

    @pytest.mark.unit
    @pytest.mark.parametrize("metadata_json", [
        (
            {
                "resolution": {"width": 120, "height": 120},
                "frame": ["something", "something2"]
            }
        ),
        (
            {
                "resolution": {"width": 120, "height": 120},
                "frame": []
            }
        ),
        (
            load_json("snapshot_pose_raw.json")
        ),
        (
            load_json("snapshot_classification_raw.json")
        )
    ])
    @patch("metadata.consumer.voxel.metadata_parser.MetadataParser.parse_frame")
    def test_parse(self, parse_frame_mock: Mock, metadata_json: dict):
        """Test the parse function"""
        # GIVEN
        list_frames_mock: list[dict] = metadata_json["frame"]
        parse_frame_mock.side_effect = list_frames_mock

        # WHEN
        result_frames = MetadataParser.parse(metadata_json)

        # THEN
        assert list_frames_mock == result_frames
        if len(list_frames_mock):
            parse_frame_mock.assert_called_with(
                ANY,
                int(metadata_json["resolution"]["width"]),
                int(metadata_json["resolution"]["height"]))

    @pytest.mark.unit
    @pytest.mark.parametrize("frame,width,height,expected_frame", [
        (
            load_json("snapshot_pose_raw.json")["frame"][0],
            1280,
            720,
            load_frame_pydantic("snapshot_pose_pydantic.json")
        ),
        (
            load_json("snapshot_classification_raw.json")["frame"][0],
            1280,
            720,
            load_frame_pydantic("snapshot_classification_pydantic.json")
        ),
        (
            load_json("snapshot_bbox_raw.json")["frame"][0],
            1280,
            720,
            load_frame_pydantic("snapshot_bbox_pydantic.json")
        )
    ])
    def test_parse_frame(self, frame: dict, width: int, height: int, expected_frame: Frame):
        """
        Test the parse_frame function
        This does not mock the other parts of the parser, and instead tests thus tests the entire parser.
        """
        # WHEN
        result_frame = MetadataParser.parse_frame(frame, width, height)

        # THEN
        assert result_frame == expected_frame

    @pytest.mark.unit
    @pytest.mark.parametrize("bool_attr,expected_classifications,expected_exception", [
        (
            [
                {
                    "name": "CameraViewShifted",
                    "value": "true"
                },
                {
                    "name": "CameraViewBlocked",
                    "value": "false"
                },
            ],
            [
                Classification(name="CameraViewShifted", value=1.0),
                Classification(name="CameraViewBlocked", value=0.0),
            ],
            None
        ),
        (
            [],
            [],
            None
        ),
        (
            [
                {
                    "name": "CameraViewShifted",
                    "value": "not_false"
                },
                {
                    "name": "CameraViewBlocked",
                    "value": "false"
                },
            ],
            [
                Classification(name="CameraViewShifted", value=1.0),
                Classification(name="CameraViewBlocked", value=0.0),
            ],
            ValueError
        )
    ])
    def test_parse_bool_attributes(
            self,
            bool_attr: list,
            expected_classifications: list[Classification],
            expected_exception: Exception):
        """
        Test the parse_bool_attributes function
        """
        # WHEN
        if expected_exception is None:
            result_classifications = MetadataParser.parse_bool_attributes(bool_attr)

            # THEN
            assert result_classifications == expected_classifications
        else:
            with pytest.raises(expected_exception):  # type: ignore
                MetadataParser.parse_bool_attributes(bool_attr)
            return

    @pytest.mark.unit
    @pytest.mark.parametrize("float_attr,expected_classifications", [
        (
            [
                {
                    "name": "CameraViewShifted",
                    "value": "1.5"
                },
                {
                    "name": "CameraViewBlocked",
                    "value": "0.9123"
                },
            ],
            [
                Classification(name="CameraViewShifted", value=1.5),
                Classification(name="CameraViewBlocked", value=0.9123),
            ]
        ),
        (
            [],
            []
        )
    ])
    def test_parse_float_attributes(self, float_attr: list, expected_classifications: list[Classification]):
        """
        Test the parse_float_attributes function
        """
        # WHEN
        result_classifications = MetadataParser.parse_float_attributes(float_attr)

        # THEN
        assert result_classifications == expected_classifications

    @pytest.mark.unit
    @pytest.mark.parametrize("person_details,expected_person", [
        (
            load_json("snapshot_pose_raw.json")["frame"][0]["objectlist"][0]["personDetail"],
            load_frame_pydantic("snapshot_pose_pydantic.json").persons[0]
        )
    ])
    def test_parse_person_details(self, person_details: list, expected_person: Person):
        """
        Test the parse_person_details function
        """
        # WHEN
        result_person = MetadataParser.parse_person_details(person_details, 0)

        # THEN
        assert result_person == expected_person

    @pytest.mark.unit
    @pytest.mark.parametrize("bbox_json,expected_bbox", [
        (
            load_json("snapshot_pose_raw.json")["frame"][0]["objectlist"][0],
            BoundingBox(x=0, y=0, width=0, height=0, confidence=0.0, name="16")
        ),
        (
            {
                "id": "26",
                "confidence": "0.5",
                "box": {
                    "height": "20",
                    "width": "20",
                    "x": "1",
                    "y": "3"
                }
            },
            BoundingBox(x=1, y=3, width=20, height=20, confidence=0.5, name="26")
        )
    ])
    def test_parse_detection_box(self, bbox_json: dict, expected_bbox: BoundingBox):
        """
        Test the parse_bounding_box function
        """
        # WHEN
        result_bbox = MetadataParser.parse_detection_box(bbox_json)

        # THEN
        assert result_bbox == expected_bbox
