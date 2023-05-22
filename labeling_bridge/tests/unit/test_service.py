"""Test Labelling Bridge service"""
import sys
from unittest.mock import Mock

import pytest

from labeling_bridge.service import ApiService

# pylint: disable=missing-function-docstring, missing-module-docstring, missing-class-docstring, too-few-public-methods
# pylint: disable=redefined-outer-name, protected-access


@pytest.fixture
def api_service():
    s3_mock = Mock()
    container_services = Mock()
    return ApiService(s3_mock, container_services)


@pytest.fixture
def fiftyone_fo():
    fiftyone_module = sys.modules["fiftyone"]
    fiftyone_module.Dataset = Mock()

    return fiftyone_module


@pytest.fixture
def fiftyone_fosv():
    fosv = sys.modules["fiftyone.server.view"]

    return fosv


def message(method, tag=None, filters=None, stages=None):
    return {
        "dataset": "dummy_dataset",
        "kognicProjectId": "dummy_project",
        "labellingType": "Splines",
        "labellingJobName": "dummy_batch",
        "labellingGuidelines": "dummy_guides",
        "voxelExportMethod": method,
        "clientId": "11111",
        "clientSecret": "aaaaa",
        "voxelTagToExport": tag,
        "filters": filters,
        "stages": stages
    }


@pytest.mark.unit
def test_get_s3_path_parts(api_service: ApiService):
    # GIVEN
    s3_path = "s3://foo-team-raw/samples/example_dataset/example_1.png"
    expected_bucket = "foo-team-raw"
    expected_key = "samples/example_dataset/example_1.png"
    # WHEN
    bucket, key = api_service._get_s3_path_parts(s3_path)  # pylint: disable=protected-access
    # THEN
    assert bucket == expected_bucket
    assert key == expected_key


@pytest.mark.unit
@pytest.mark.parametrize("input_message",
                         [(message("tag", tag="dummy_tag")),
                          (message("filter", filters={"filter_1": "value_1", "filter_2": "value_2"},
                                   stages={"stage_1": "value_1", "stage_2": "value_2"}))
                          ])
def test_parse_request(input_message, api_service: ApiService):
    # WHEN
    api_service._parse_request(input_message)
    # THEN
    assert api_service.dataset_name == input_message["dataset"]
    assert api_service.kognic_project_id == input_message["kognicProjectId"]
    assert api_service.labelling_type == input_message["labellingType"]
    assert api_service.labelling_job_name == input_message["labellingJobName"]
    assert api_service.labelling_guidelines == input_message["labellingGuidelines"]
    assert api_service.export_method == input_message["voxelExportMethod"]
    assert api_service.client_id == input_message["clientId"]
    assert api_service.client_secret == input_message["clientSecret"]
    assert api_service.tag == input_message["voxelTagToExport"]
    assert api_service.filters == input_message["filters"]
    assert api_service.stages == input_message["stages"]
