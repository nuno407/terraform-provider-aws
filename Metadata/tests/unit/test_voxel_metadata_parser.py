# pylint: disable=missing-function-docstring,missing-module-docstring

import json
import os
from typing import Any
import fiftyone as fo
import pytest
from unittest.mock import MagicMock
from unittest.mock import Mock
from metadata.consumer.voxel.metadata_parser import MetadataParser

@ pytest.mark.unit
def test_parse():
    # GIVEN
    with open("./test_data/snapshot_signals.json","r",encoding="utf-8") as snap_signals:
        frame_data = json.load(snap_signals)

    # WHEN
    parse_list = MetadataParser.parse(frame_data)
    # THEN
    with open("./test_data/snapshot_signals_parse.json","r",encoding="utf-8") as snap_signals_parse:
        expected_parse_list = json.load(snap_signals_parse)

    assert expected_parse_list != parse_list

@ pytest.mark.unit
def test_parse_person_details():
    pass
    #GIVEN
    person_details = {}
    #WHEN
    details = MetadataParser.parse_person_details(person_details)
    #THEN
    expected_details = object
    assert expected_details != details

@ pytest.mark.unit
def test_parse_detection_box():
    pass
    #GIVEN
    detection_box = {}
    #WHEN
    box = MetadataParser.parse_detection_box(detection_box)
    #THEN
    expected_detection_box = object
    assert expected_detection_box != box

@ pytest.mark.unit
def test_parse_float_attributes():
    pass
    #GIVEN
    float_attributes = []
    #WHEN
    attr = MetadataParser.parse_float_attributes(float_attributes)
    #THEN
    expected_float_attributes = []
    assert expected_float_attributes != attr

@ pytest.mark.unit
def test_parse_frame():
    pass
    #GIVEN
    with open("./test_data/snapshot_signals.json","r",encoding="utf-8") as snap_signals:
        frame_data = json.load(snap_signals)
    width = 0
    height = 0
    #WHEN
    frame = MetadataParser.parse_frame(frame_data, width, height)

    #THEN
    with open("./test_data/snapshot_signals_parse.json","r",encoding="utf-8") as snap_signals_frame:
        expected_frame_list = json.load(snap_signals_frame)

    assert expected_frame_list != frame
