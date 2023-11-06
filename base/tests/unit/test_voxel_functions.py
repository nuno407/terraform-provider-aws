"""Test Fiftyone importer"""
import sys
from unittest.mock import Mock, call, MagicMock, patch

import pytest
from pytest_mock import MockerFixture
import fiftyone as fo
from kink import di

from base.model.config.policy_config import PolicyConfig
from base.voxel.functions import delete_sample, find_or_create_sample, set_field, _construct_raw_filepath_from_filepath, set_mandatory_fields_on_sample


# pylint: disable=missing-function-docstring, missing-module-docstring, missing-class-docstring, too-few-public-methods, protected-access
# pylint: disable=redefined-outer-name, invalid-name
# mypy: disable-error-code=assignment


di[PolicyConfig] = PolicyConfig(default_policy_document="TestDoc", policy_document_per_tenant={})


@pytest.fixture
def fiftyone():
    fo = sys.modules["fiftyone"]
    fo.Dataset = Mock()

    return fo


@pytest.fixture(name="voxel_base_function_find_sample")
def fixture_voxel_base_function_set_field(mocker: MockerFixture) -> Mock:
    """mocks voxel base function find_sample"""
    return mocker.patch("base.voxel.functions.find_sample")


@pytest.mark.unit
def test_delete_existing_sample(voxel_base_function_find_sample: Mock):
    # GIVEN
    sample = MagicMock()
    sample.has_field.return_value = False
    voxel_base_function_find_sample.return_value = sample
    dataset = Mock()

    # WHEN
    delete_sample(dataset, "/foo/bar")

    # THEN
    voxel_base_function_find_sample.assert_called_once_with(dataset, "/foo/bar")
    dataset.delete_samples.assert_called_once_with([sample])


@pytest.mark.unit
def test_delete_missing_sample(voxel_base_function_find_sample: Mock):
    # GIVEN
    voxel_base_function_find_sample.side_effect = ValueError("Not found")
    dataset = MagicMock()
    dataset.delete_samples = MagicMock()

    # WHEN
    delete_sample(dataset, "/foo/bar")

    # THEN
    voxel_base_function_find_sample.assert_called_once_with(dataset, "/foo/bar")
    dataset.delete_samples.assert_not_called()


@pytest.mark.unit
def test_find_or_create_sample_with_existing_sample():
    # GIVEN
    sample = MagicMock()
    dataset = MagicMock()
    dataset.one.return_value = sample

    # WHEN
    found_sample = find_or_create_sample(dataset, "/foo/bar.jpg")

    # THEN
    assert sample == found_sample


@pytest.mark.unit
def test_find_or_create_sample_with_no_sample(voxel_base_function_find_sample: Mock):
    # GIVEN
    voxel_base_function_find_sample.side_effect = ValueError("Not found")
    dataset = MagicMock()

    # WHEN
    found_sample = find_or_create_sample(dataset, "/foo/bar.jpg")

    # THEN
    assert found_sample is not None
    dataset.add_sample.assert_called()


@pytest.mark.unit
@pytest.mark.parametrize("values, expected_sample, sample_to_test", [
    (
        {"test": "value", "testnested": {"test1": "value1"}},
        fo.Sample(
            filepath="test_path",
            test="value",
            testnested=fo.DynamicEmbeddedDocument(test1="value1")
        ),
        fo.Sample(
            filepath="test_path",
        )
    )
]
)
def test_set_field(values, expected_sample: fo.Sample, sample_to_test: fo.Sample):
    for key, value in values.items():
        set_field(sample_to_test, key, value)
        assert sample_to_test[key] == expected_sample[key]


@pytest.mark.unit
@pytest.mark.parametrize("filepath,raw_filepath_expected", [
    ("s3://dev-proj-anonymized/samples/dataset/test_anonymized.png",
     "s3://dev-proj-raw/samples/dataset/test.png"),
    ("s3://dev-proj-anonymized/anonymized/anonymized/anonymized_anonymized.png",
     "s3://dev-proj-raw/anonymized/anonymized/anonymized.png"),
    ("s3://dev-proj-anonymized-videos/samples/dataset/test_anonymized.png",
     "s3://dev-proj-raw-videos/samples/dataset/test.png"),
    ("s3://dev-proj-raw/samples/dataset/test.png",
     "s3://dev-proj-raw/samples/dataset/test.png"),
    ("s3://dev-rcd-anonymized-video-files/samples/dataset/test_anonymized.png",
     "s3://dev-rcd-raw-video-files/samples/dataset/test.png")
]
)
def test_get_raw_filepath_from_filepath_str(filepath, raw_filepath_expected):

    # WHEN
    raw_filepath = _construct_raw_filepath_from_filepath(filepath)

    # THEN
    assert raw_filepath == raw_filepath_expected


@pytest.mark.unit
def test_set_mandatory_fields_on_sample():

    sample = MagicMock()
    sample.has_field.return_value = False
    sample.filepath = "s3://dev-rcd-anonymized-video-files/samples/dataset/test_anonymized.png"

    # WHEN
    set_mandatory_fields_on_sample(sample=sample, tenant_id="tenant_id")

    # THEN
    sample.has_field.assert_has_calls([call("raw_filepath"), call("data_privacy_document_id")])
    sample.set_field.assert_has_calls([
        call("raw_filepath", "s3://dev-rcd-raw-video-files/samples/dataset/test.png", dynamic=True),
        call("data_privacy_document_id", "TestDoc", dynamic=True)
    ])
