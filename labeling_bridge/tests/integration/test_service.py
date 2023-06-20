"""Test Labelling Bridge service"""
import json
import os
import unittest
from unittest.mock import Mock, patch, ANY
import fiftyone as fo

import pytest

from labeling_bridge.service import ApiService

# pylint: disable=missing-function-docstring, missing-module-docstring, missing-class-docstring, too-few-public-methods
# pylint: disable=redefined-outer-name, protected-access

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))


@pytest.fixture
def sample_dataset():
    fo.delete_non_persistent_datasets()
    return fo.Dataset(name="dummy_dataset")


@pytest.fixture
def api_service():
    s3_mock = Mock()
    container_services = Mock()
    return ApiService(s3_mock, container_services)


@pytest.fixture
def semseg_test_data():
    with (open(os.path.join(__location__, "test_data/test.json"), "r")) as file:
        data_string = file.read()
        data_string.replace("__location__", __location__)
        data = json.loads(data_string)
    return data


@pytest.fixture
def semseg_annotation(semseg_test_data):
    semseg = Mock()
    semseg.content = semseg_test_data
    return semseg


@pytest.fixture
def import_message():
    return {
        "kognicProjectId": "dummy_project",
        "labellingJobName": "dummy_batch",
        "clientId": "11111",
        "clientSecret": "aaaaa",
        "dataset": "dummy_dataset"
    }


def create_sample(dataset, filename, tags=None):
    path = f"{__location__}/test_data/{filename}"
    sample = fo.Sample(filepath=path, tags=tags)
    sample["raw_filepath"] = path
    dataset.add_sample(sample)
    return sample


@pytest.mark.integration
def test_kognic_import_success(api_service: ApiService, semseg_annotation, import_message, sample_dataset):
    # GIVEN
    kognic_factory = Mock()
    api_service.kognic_interface_factory = Mock(return_value=kognic_factory)
    kognic_factory.get_annotation_types = Mock(return_value=["2D_semseg"])
    kognic_factory.get_project_annotations = Mock(return_value=[semseg_annotation])

    sample_with_labeling_data = create_sample(sample_dataset, "test.png")
    untouched_sample = create_sample(sample_dataset, "test2.png")

    # WHEN
    api_service.kognic_import(request_data=import_message)

    # THEN
    assert sample_with_labeling_data.segmentations is not None
    assert untouched_sample.segmentations is None


@pytest.mark.integration
@unittest.mock.patch("labeling_bridge.service._logger")
def test_kognic_import_error(_logger, api_service: ApiService, semseg_annotation, import_message):
    # GIVEN
    kognic_factory = Mock()
    api_service.kognic_interface_factory = Mock(return_value=kognic_factory)
    kognic_factory.get_annotation_types = Mock(return_value=["Splines"])
    kognic_factory.get_project_annotations = Mock(side_effect=Exception("Unknown issue occurred"))

    # WHEN
    api_service.kognic_import(request_data=import_message)

    # THEN
    _logger.exception.assert_called_once()


def export_message(method, tag=None, filters=None, stages=None):
    return {
        "dataset": "dummy_dataset",
        "kognicProjectId": "dummy_project",
        "labellingType": "2D_semseg",
        "labellingJobName": "dummy_batch",
        "labellingGuidelines": "dummy_guides",
        "voxelExportMethod": method,
        "clientId": "11111",
        "clientSecret": "aaaaa",
        "voxelTagToExport": tag,
        "filters": filters,
        "stages": stages
    }


@pytest.mark.integration
@patch("base.aws.container_services")
def test_kognic_export_success(container_services, api_service: ApiService, semseg_annotation, sample_dataset):
    # GIVEN
    kognic_factory = Mock()
    api_service.kognic_interface_factory = Mock(return_value=kognic_factory)
    api_service._get_s3_path_parts = Mock(return_value=["dev-rcd-raw",  # type: ignore[method-assign]
                                                        "path/to/file.png"])
    kognic_factory.verify_batch = Mock(return_value=False)
    kognic_factory.create_batch = Mock()
    kognic_factory.upload_image = Mock()
    container_services.download_file_to_disk = Mock()

    exported_sample = create_sample(sample_dataset, "test.png", tags=["export-tag"])
    _additional_sample = create_sample(sample_dataset, "test2.png")

    msg = export_message("tag", tag="export-tag")
    # WHEN
    api_service.kognic_export(request_data=msg)

    # THEN
    kognic_factory.upload_image.assert_called_once_with("dummy_project", "dummy_batch", "2D_semseg",
                                                        exported_sample.filepath, ANY)
