"Test Service component."
# pylint: disable=missing-function-docstring,missing-module-docstring

import json
import os
from typing import Tuple
from unittest.mock import MagicMock
from unittest.mock import Mock

import pytest

from metadata.api.service import ApiService, MONGODB_PIPELINE_PREFIX_ADD_START_AT_END_AT

s3 = MagicMock()
db = MagicMock()
service = ApiService(db, s3)


@pytest.mark.unit
def test_create_video_url():
    # GIVEN
    file = "bar.mp4"
    folder = "foo/"
    bucket = "baz"
    url = "http://spam.egg/baz/"
    s3.generate_presigned_url = Mock(return_value=url)

    # WHEN
    return_value = service.create_video_url(bucket, folder, file)

    # THEN
    assert return_value == url
    s3.generate_presigned_url.assert_called_once()
    args = s3.generate_presigned_url.call_args.args
    assert args[0] == "get_object"
    kwargs = s3.generate_presigned_url.call_args.kwargs
    params = kwargs["Params"]
    assert params["Bucket"] == bucket
    assert params["Key"] == folder + file


@pytest.mark.unit
def test_create_anonymized_video_url():
    # GIVEN
    recording_id = "qux"
    path = "foobar.mp4"
    bucket = "baz"
    url = "http://spam.egg/baz/"
    db.get_algo_output = Mock(
        return_value={"output_paths": {"video": bucket + "/" + path}})
    s3.generate_presigned_url = Mock(return_value=url)

    # WHEN
    return_value = service.create_anonymized_video_url(recording_id)

    # THEN
    # db-part
    db.get_algo_output.assert_called_once_with("Anonymize", recording_id)
    # s3-part
    assert return_value == url
    s3.generate_presigned_url.assert_called_once()
    args = s3.generate_presigned_url.call_args.args
    assert args[0] == "get_object"
    kwargs = s3.generate_presigned_url.call_args.kwargs
    params = kwargs["Params"]
    assert params["Bucket"] == bucket
    assert params["Key"] == path


__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))


def read_and_parse_test_data(filepath: str) -> dict:
    """Read and parse test data.

    Args:
        filepath (str): path to test data
    Returns:
        dict of the parsed content of test data file.
    """
    with open(os.path.join(
            __location__, filepath), "r", encoding="utf-8") as file_handle:
        content = file_handle.read()
        return json.loads(content)


@pytest.mark.unit
@pytest.mark.parametrize("aggregation_result_path,get_table_kwargs,expected_result_path", [
    ("test_data/recording_aggregation_response.json",
        {
            "page_size": 10,
            "page": 1,
            "query": None,
            "operator": None,
            "sorting": None,
            "direction": None
        },
     "test_data/table_data_expected.json"),
    ("test_data/recording_aggregation_response_sorted.json",
        {
            "page_size": 10,
            "page": 1,
            "query": None,
            "operator": None,
            "sorting": "length",
            "direction": "asc"
        },
     "test_data/table_data_expected_sorted.json"),
    ("test_data/recording_aggregation_response_filtered_sorted.json",
        {
            "page_size": 10,
            "page": 1,
            "query": [{"time": {">": "2022-04-01 13:00:00"}, "_id": {"has": "srx"}}],
            "operator": "and",
            "sorting": "length",
            "direction": "dsc"
        },
     "test_data/table_data_expected_filtered_sorted.json"),
])
def test_get_table_data(aggregation_result_path: str,
                        get_table_kwargs: Tuple[str, dict, str], expected_result_path: str):
    aggregation_result = read_and_parse_test_data(aggregation_result_path)
    db.get_recording_list = Mock(return_value=(aggregation_result, 1, 1))

    result, _, _ = service.get_table_data(**get_table_kwargs)

    expected = read_and_parse_test_data(expected_result_path)
    assert expected == result


@ pytest.mark.unit
def test_get_single_recording():
    jsonstr = open(os.path.join(
        __location__, "test_data/recording_aggregation_response.json"), "r").read()
    aggregation_result = json.loads(jsonstr)
    db.get_single_recording = Mock(
        side_effect=[aggregation_result[2], aggregation_result[3]])

    result = service.get_single_recording(
        "srxfut2internal20_rc_srx_qa_na_fut2_003_TrainingRecorder_1648835260452_1648835387254")

    expectedstr = open(os.path.join(
        __location__, "test_data/single_recording_expected.json"), "r").read()
    expected = json.loads(expectedstr)
    assert result == expected
    assert db.get_single_recording.call_count == 2


@ pytest.mark.unit
def test_update_video_description():
    # GIVEN
    video_id = "foo"
    description = "bar"
    db.update_recording_description = Mock()

    # WHEN
    service.update_video_description(video_id, description)

    # THEN

    db.update_recording_description.assert_called_once_with(video_id, description)


@pytest.mark.unit
def test_get_video_signals_from_InteriorRecorder_given_TrainingRecorder():
    # GIVEN
    db.get_single_recording = Mock(
        return_value={
            "recording_overview": {
                "deviceID": "TestDevice",
                "tenantID": "TestTenant"}})
    db.get_recording_list = MagicMock(return_value=([MagicMock()], 1, None))
    db.get_signals = MagicMock()
    additional_query = {"$and": [
        {"recording_overview.deviceID": "TestDevice"},
        {"recording_overview.tenantID": "TestTenant"},
        {"video_id": {"$regex": "^((?!TrainingRecorder).)*$"}},
        {"$and": [{"start_at": {"$gte": 1648835260452}}, {
            "start_at": {"$lte": 1648835260452 + 130_000}}]},
        {"$and": [{"end_at": {"$lte": 1648835260452}}, {
            "end_at": {"$gte": 1648835260452 - 130_000}}]},
    ]
    }
    # WHEN
    service.get_video_signals("srxfut2internal20_rc_srx_qa_na_fut2_003_TrainingRecorder_1648835260452_1648835260452")

    # THEN
    db.get_recording_list.assert_called_once_with(
        page_size=10,
        page=1,
        additional_query=additional_query,
        order=None,
        aggregation_pipeline_prefix=MONGODB_PIPELINE_PREFIX_ADD_START_AT_END_AT)


@pytest.mark.unit
def test_get_video_signals_from_InteriorRecorder_given_TrainingRecorder_no_video_exception():
    # GIVEN
    db.get_single_recording = Mock(
        return_value={
            "recording_overview": {
                "deviceID": "TestDevice",
                "tenantID": "TestTenant"}})
    db.get_recording_list = MagicMock(return_value=([], 0, None))
    db.get_signals = MagicMock()
    # WHEN/THEN
    with pytest.raises(LookupError):
        service.get_video_signals(
            "srxfut2internal20_rc_srx_qa_na_fut2_003_TrainingRecorder_1648835260452_1648835260452")
