""" Test for API models """
# pylint: disable=missing-class-docstring,missing-function-docstring,too-few-public-methods
from pydantic import ValidationError
import pytest
from labeling_bridge.models.api import RequestExportJobDTO, RequestImportJobDTO


def message_export(method, labeling_types, tag=None, filters=None, stages=None):
    return {
        "dataset": "dummy_dataset",
        "kognicProjectId": "dummy_project",
        "labellingType": labeling_types,
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
@pytest.mark.parametrize(
    "input_message", [
        (message_export(
            "tag", labeling_types=["Splines"], tag="BLA")), (message_export(
                "filter", labeling_types=[
                    "Splines", "2D_semseg"], filters={
                        "filter_1": "value_1", "filter_2": "value_2"}, stages=[{
                            "stage_1": "value_1", "stage_2": "value_2"}]))])
def test_parse_export_request(input_message):
    # WHEN
    test_obj = RequestExportJobDTO(**input_message)
    # THEN
    assert test_obj.dataset_name == input_message["dataset"]
    assert test_obj.kognic_project_id == input_message["kognicProjectId"]
    assert [e.value for e in test_obj.labelling_types] == input_message["labellingType"]
    assert test_obj.labelling_job_name == input_message["labellingJobName"]
    assert test_obj.labelling_guidelines == input_message["labellingGuidelines"]
    assert test_obj.voxel_export_method == input_message["voxelExportMethod"]
    assert test_obj.client_id == input_message["clientId"]
    assert test_obj.client_secret == input_message["clientSecret"]
    assert test_obj.tag == input_message["voxelTagToExport"]
    assert test_obj.filters == input_message["filters"]
    assert test_obj.stages == input_message["stages"]


@pytest.mark.unit
def test_parse_bad_export_request():
    with pytest.raises(ValidationError):
        _ = RequestExportJobDTO(**message_export("tag", labeling_types=["NOT_A_VALID_TAG"], tag=["TEST"]))


@pytest.mark.unit
def test_parse_import_request():
    # WHEN
    test_obj = RequestImportJobDTO(**{
        "dataset": "dummy_dataset",
        "kognicProjectId": "dummy_project",
        "labellingJobName": "dummy_batch",
        "clientId": "11111",
        "clientSecret": "aaaaa"
    })
    # THEN
    assert test_obj.dataset_name == "dummy_dataset"
    assert test_obj.kognic_project_id == "dummy_project"
    assert test_obj.labelling_job_name == "dummy_batch"
    assert test_obj.client_id == "11111"
    assert test_obj.client_secret == "aaaaa"


@pytest.mark.unit
def test_parse_bad_import_request():
    with pytest.raises(ValidationError):
        _ = RequestImportJobDTO(**{"bla": "ble"})
