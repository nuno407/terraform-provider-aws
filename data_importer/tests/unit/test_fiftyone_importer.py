"""Test Fiftyone importer"""
import sys
from unittest.mock import Mock, call, MagicMock

import pytest
import fiftyone as fo

from data_importer.fiftyone_importer import FiftyoneImporter


# pylint: disable=missing-function-docstring, missing-module-docstring, missing-class-docstring, too-few-public-methods
# pylint: disable=redefined-outer-name, invalid-name
# mypy: disable-error-code=assignment


@pytest.fixture
def fiftyone():
    fo = sys.modules["fiftyone"]
    fo.Dataset = Mock()
    return fo


@pytest.fixture
def importer():
    return FiftyoneImporter()


@pytest.mark.unit
def test_create_dataset(fiftyone, importer: FiftyoneImporter):
    # GIVEN
    fiftyone.dataset_exists = Mock(return_value=False)

    # WHEN
    dataset = importer.load_dataset("test", ["test-tag"])

    # THEN
    fiftyone.Dataset.assert_called_once_with("test", persistent=True, overwrite=False)
    fiftyone.load_dataset.assert_not_called()
    assert dataset.tags == ["test-tag"]


@pytest.mark.unit
def test_load_existing_dataset(fiftyone, importer: FiftyoneImporter):
    # GIVEN
    dataset = Mock()
    fiftyone.dataset_exists = Mock(return_value=True)
    fiftyone.load_dataset = Mock(return_value=dataset)

    # WHEN
    found_dataset = importer.load_dataset("test", ["test-tag"])

    # THEN
    fiftyone.dataset_exists.assert_called_once_with("test")
    fiftyone.load_dataset.assert_called_once_with("test")
    assert found_dataset == dataset
    fiftyone.Dataset.assert_not_called()


@pytest.mark.unit
def test_replace_existing_sample(importer: FiftyoneImporter):
    # GIVEN
    sample = Mock()
    importer.find_sample = Mock(return_value=sample)
    importer.override_metadata = Mock()
    dataset = Mock()

    # WHEN
    importer.replace_sample(dataset, "/foo/bar", {"tst": "label"})

    # THEN
    dataset.add_sample.assert_not_called()

    importer.find_sample.assert_called_once_with(dataset, "/foo/bar")
    importer.override_metadata.assert_called_once_with(sample, {"tst": "label"})
    sample.save.assert_called_once()


@pytest.mark.unit
def test_replace_new_sample(fiftyone, importer: FiftyoneImporter):
    # GIVEN
    sample = Mock()
    importer.find_sample = Mock(side_effect=ValueError("Not found"))
    importer.override_metadata = Mock()
    fiftyone.Sample = Mock(return_value=sample)
    dataset = Mock()

    # WHEN
    importer.replace_sample(dataset, "/foo/bar", {})

    # THEN
    fiftyone.Sample.assert_called_with("/foo/bar")
    dataset.add_sample.assert_called_once_with(sample)

    importer.find_sample.assert_called_once_with(dataset, "/foo/bar")
    importer.override_metadata.assert_called_once_with(sample, {})
    sample.save.assert_called_once()


@pytest.mark.unit
def test_update_existing_sample(importer: FiftyoneImporter):
    # GIVEN
    sample = Mock()
    importer.find_sample = Mock(return_value=sample)
    importer.override_metadata = Mock()
    dataset = Mock()

    # WHEN
    importer.upsert_sample(dataset, "/foo/bar", {"tst": "label", "tst2": "label2"})

    # THEN
    dataset.add_sample.assert_not_called()

    importer.find_sample.assert_called_once_with(dataset, "/foo/bar")
    importer.override_metadata.assert_not_called()
    sample.set_field.assert_has_calls([call("tst", "label"), call("tst2", "label2")])
    importer.override_metadata.assert_not_called()
    sample.save.assert_called_once()


@pytest.mark.unit
def test_update_new_sample(fiftyone, importer: FiftyoneImporter):
    # GIVEN
    sample = Mock()
    importer.find_sample = Mock(side_effect=ValueError("Not found"))
    importer.override_metadata = Mock()
    fiftyone.Sample = Mock(return_value=sample)
    dataset = Mock()

    # WHEN
    importer.upsert_sample(dataset, "/foo/bar", {"tst": "label", "tst2": "label2"})

    # THEN
    fiftyone.Sample.assert_called_with("/foo/bar")
    dataset.add_sample.assert_called_once_with(sample)

    importer.find_sample.assert_called_once_with(dataset, "/foo/bar")
    importer.override_metadata.assert_not_called()
    sample.set_field.assert_has_calls([call("tst", "label"), call("tst2", "label2")])
    importer.override_metadata.assert_not_called()
    sample.save.assert_called_once()


@pytest.mark.unit
def test_update_new_sample2(fiftyone, importer: FiftyoneImporter):
    # GIVEN
    sample = MagicMock()
    importer.find_sample = Mock(side_effect=ValueError("Not found"))
    importer.override_metadata = Mock()
    fiftyone.Sample = Mock(return_value=sample)
    dataset = Mock()
    nested = Mock()
    fiftyone.DynamicEmbeddedDocument = Mock(return_value=nested)
    test_dict = {"test": "value", "test2": "value2"}

    # WHEN
    importer.upsert_sample(dataset, "/foo/bar", {"tst": test_dict, "tst2": "label2"})

    # THEN
    fiftyone.Sample.assert_called_with("/foo/bar")
    dataset.add_sample.assert_called_once_with(sample)

    importer.find_sample.assert_called_once_with(dataset, "/foo/bar")
    importer.override_metadata.assert_not_called()

    sample.set_field.assert_has_calls([call("tst", nested), call("tst2", "label2")])
    fiftyone.DynamicEmbeddedDocument.assert_called_once_with(**test_dict)

    importer.override_metadata.assert_not_called()
    sample.save.assert_called_once()


@pytest.mark.unit
def test_delete_existing_sample(importer: FiftyoneImporter):
    # GIVEN
    sample = Mock()
    importer.find_sample = Mock(return_value=sample)
    dataset = Mock()

    # WHEN
    importer.delete_sample(dataset, "/foo/bar")

    # THEN
    importer.find_sample.assert_called_once_with(dataset, "/foo/bar")
    dataset.delete_samples.assert_called_once_with([sample])


@pytest.mark.unit
def test_delete_missing_sample(importer: FiftyoneImporter):
    # GIVEN
    importer.find_sample = Mock(side_effect=ValueError("Not found"))
    dataset = Mock()

    # WHEN
    importer.delete_sample(dataset, "/foo/bar")

    # THEN
    importer.find_sample.assert_called_once_with(dataset, "/foo/bar")
    dataset.delete_samples.assert_not_called()


@pytest.mark.unit
def test_find_sample(importer: FiftyoneImporter):
    # GIVEN
    sample = Mock()
    dataset = Mock()
    dataset.one = Mock(return_value=sample)

    # WHEN
    found_sample = importer.find_sample(dataset, "/foo/bar.jpg")

    # THEN
    assert sample == found_sample


@pytest.mark.unit
def test_override_metadata(importer: FiftyoneImporter):
    # GIVEN
    sample = Mock()
    sample.set_field = Mock()
    importer.delete_metadata = Mock()

    # WHEN
    importer.override_metadata(sample, {"foo": "bar", "boo": "baz"})

    # THEN
    importer.delete_metadata.assert_called_once_with(sample)
    sample.set_field.assert_has_calls([call("foo", "bar"), call("boo", "baz")])


@pytest.mark.unit
def test_delete_metadata(importer: FiftyoneImporter):
    # GIVEN
    sample = Mock()
    sample.clear_field = Mock()
    sample.iter_fields = Mock(return_value={"foo": "bar", "boo": "baz", "filepath": "/test/path.jpg"}.items())
    importer.default_sample_fields = ["filepath", "metadata", "id"]

    # WHEN
    importer.delete_metadata(sample)

    # THEN
    sample.clear_field.assert_has_calls([call("foo"), call("boo")])


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
def test_set_field(importer: FiftyoneImporter, values, expected_sample: fo.Sample, sample_to_test: fo.Sample):

    for key, value in values.items():
        importer._set_field(sample_to_test, key, value)  # pylint: disable=protected-access
        assert sample_to_test[key] == expected_sample[key]
