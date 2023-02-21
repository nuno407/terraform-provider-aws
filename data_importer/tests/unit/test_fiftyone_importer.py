import sys
from unittest.mock import Mock, MagicMock, patch, call

import pytest

# Mock fiftyone package, to prevent it from trying to establish DB connection
sys.modules['fiftyone'] = MagicMock()
from data_importer.fiftyone_importer import FiftyoneImporter


@pytest.fixture
def fo():
    fo = sys.modules['fiftyone']
    fo.Dataset = Mock()
    return fo


@pytest.fixture
def importer():
    return FiftyoneImporter()


@pytest.mark.unit
def test_create_dataset(fo: Mock, importer: FiftyoneImporter):
    # GIVEN
    fo.dataset_exists = Mock(return_value=False)

    # WHEN
    dataset = importer.load_dataset("test", ["test-tag"])

    # THEN
    fo.Dataset.assert_called_once_with("test", persistent=True, overwrite=False)
    fo.load_dataset.assert_not_called()
    assert dataset.tags == ["test-tag"]


@pytest.mark.unit
def test_load_existing_dataset(fo: Mock, importer: FiftyoneImporter):
    # GIVEN
    dataset = Mock()
    fo.dataset_exists = Mock(return_value=True)
    fo.load_dataset = Mock(return_value=dataset)

    # WHEN
    found_dataset = importer.load_dataset("test", ["test-tag"])

    # THEN
    fo.dataset_exists.assert_called_once_with("test")
    fo.load_dataset.assert_called_once_with("test")
    assert found_dataset == dataset
    fo.Dataset.assert_not_called()


@pytest.mark.unit
def test_update_existing_sample(fo: Mock, importer: FiftyoneImporter):
    # GIVEN
    sample = Mock()
    importer.find_sample = Mock(return_value=sample)
    importer.override_metadata = Mock()
    dataset = Mock()

    # WHEN
    importer.upsert_sample(dataset, "/foo/bar", {})

    # THEN
    dataset.add_sample.assert_not_called()

    importer.find_sample.assert_called_once_with(dataset, "/foo/bar")
    importer.override_metadata.assert_called_once_with(sample, {})
    sample.save.assert_called_once()


@pytest.mark.unit
def test_update_new_sample(fo: Mock, importer: FiftyoneImporter):
    # GIVEN
    sample = Mock()
    importer.find_sample = Mock(side_effect=ValueError('Not found'))
    importer.override_metadata = Mock()
    fo.Sample = Mock(return_value=sample)
    dataset = Mock()

    # WHEN
    importer.upsert_sample(dataset, "/foo/bar", {})

    # THEN
    fo.Sample.assert_called_with("/foo/bar")
    dataset.add_sample.assert_called_once_with(sample)

    importer.find_sample.assert_called_once_with(dataset, "/foo/bar")
    importer.override_metadata.assert_called_once_with(sample, {})
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
