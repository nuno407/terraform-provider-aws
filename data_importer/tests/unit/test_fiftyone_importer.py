"""Test Fiftyone importer"""
import sys
from unittest.mock import Mock, call, MagicMock

import pytest
import fiftyone as fo

from data_importer.fiftyone_importer import FiftyoneImporter


# pylint: disable=missing-function-docstring, missing-module-docstring, missing-class-docstring, too-few-public-methods, protected-access
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
    sample.filepath = "s3://dev-proj-anonymized/samples/dataset/test_anonymized.png"
    importer.find_sample = Mock(return_value=sample)
    importer.override_metadata = Mock()
    dataset = Mock()

    # WHEN
    importer.replace_sample(dataset, "/foo/bar", {"tst": "label"})

    # THEN
    dataset.add_sample.assert_not_called()
    importer.find_sample.assert_called_once_with(dataset, "/foo/bar")
    importer.override_metadata.assert_called_once_with(sample, {"tst": "label"})
    sample.set_field.assert_called_with(
        "raw_filepath",
        "s3://dev-proj-raw/samples/dataset/test.png",
        dynamic=True)
    sample.save.assert_called_once()


@pytest.mark.unit
def test_replace_new_sample(fiftyone, importer: FiftyoneImporter):
    # GIVEN
    sample = Mock()
    sample.filepath = "s3://dev-proj-anonymized/samples/dataset/test_anonymized.png"
    importer.find_sample = Mock(side_effect=ValueError("Not found"))
    importer.override_metadata = Mock()
    fiftyone.Sample = Mock(return_value=sample)
    dataset = Mock()

    # WHEN
    importer.replace_sample(dataset, "/foo/bar", {})

    # THEN
    fiftyone.Sample.assert_called_with("/foo/bar")
    dataset.add_sample.assert_called_once_with(sample, dynamic=True)

    importer.find_sample.assert_called_once_with(dataset, "/foo/bar")
    importer.override_metadata.assert_called_once_with(sample, {})
    sample.set_field.assert_called_with(
        "raw_filepath",
        "s3://dev-proj-raw/samples/dataset/test.png",
        dynamic=True)
    sample.save.assert_called_once()


@pytest.mark.unit
def test_update_existing_sample(importer: FiftyoneImporter):
    # GIVEN
    sample = Mock()
    sample.filepath = "s3://dev-proj-anonymized/samples/dataset/test_anonymized.png"
    importer.find_sample = Mock(return_value=sample)
    importer.override_metadata = Mock()

    dataset = Mock()

    # WHEN
    importer.upsert_sample(dataset, "/foo/bar", {"tst": "label", "tst2": "label2"})

    # THEN
    dataset.add_sample.assert_not_called()

    importer.find_sample.assert_called_once_with(dataset, "/foo/bar")
    importer.override_metadata.assert_not_called()
    sample.set_field.assert_has_calls([call("tst", "label", dynamic=True), call(
        "tst2", "label2", dynamic=True)])
    importer.override_metadata.assert_not_called()
    sample.set_field.assert_called_with(
        "raw_filepath",
        "s3://dev-proj-raw/samples/dataset/test.png",
        dynamic=True)
    sample.save.assert_called_once()


@pytest.mark.unit
def test_update_new_sample(fiftyone, importer: FiftyoneImporter):
    # GIVEN
    sample = Mock()
    sample.filepath = "s3://dev-proj-anonymized/samples/dataset/test_anonymized.png"
    importer.find_sample = Mock(side_effect=ValueError("Not found"))
    importer.override_metadata = Mock()
    fiftyone.Sample = Mock(return_value=sample)
    dataset = Mock()

    # WHEN
    importer.upsert_sample(dataset, "/foo/bar", {"tst": "label", "tst2": "label2"})

    # THEN
    fiftyone.Sample.assert_called_with("/foo/bar")
    dataset.add_sample.assert_called_once_with(sample, dynamic=True)

    importer.find_sample.assert_called_once_with(dataset, "/foo/bar")
    importer.override_metadata.assert_not_called()
    sample.set_field.assert_has_calls([call("tst", "label", dynamic=True), call("tst2", "label2", dynamic=True)])
    importer.override_metadata.assert_not_called()
    sample.set_field.assert_called_with(
        "raw_filepath",
        "s3://dev-proj-raw/samples/dataset/test.png",
        dynamic=True)
    sample.save.assert_called_once()


@pytest.mark.unit
def test_update_new_sample2(fiftyone, importer: FiftyoneImporter):
    # GIVEN
    sample = MagicMock()
    sample.filepath = "s3://dev-proj-anonymized/samples/dataset/test_anonymized.png"
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
    dataset.add_sample.assert_called_once_with(sample, dynamic=True)

    importer.find_sample.assert_called_once_with(dataset, "/foo/bar")
    importer.override_metadata.assert_not_called()

    sample.set_field.assert_has_calls([call("tst", nested, dynamic=True), call("tst2", "label2", dynamic=True)])
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
    sample.set_field.assert_has_calls([call("foo", "bar", dynamic=True), call("boo", "baz", dynamic=True)])


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


@pytest.mark.unit
@pytest.mark.parametrize("filepath,raw_filepath_expected", [
    ("s3://dev-proj-anonymized/samples/dataset/test_anonymized.png",
     "s3://dev-proj-raw/samples/dataset/test.png"),
    ("s3://dev-proj-anonymized/anonymized/anonymized/anonymized_anonymized.png",
     "s3://dev-proj-raw/anonymized/anonymized/anonymized.png"),
    ("s3://dev-proj-anonymized-videos/samples/dataset/test_anonymized.png",
     "s3://dev-proj-raw-videos/samples/dataset/test.png"),
    ("s3://dev-proj-raw/samples/dataset/test.png",
     "s3://dev-proj-raw/samples/dataset/test.png")]
)
def test_get_raw_filepath_from_filepath_str(importer: FiftyoneImporter, filepath, raw_filepath_expected):

    # WHEN
    raw_filepath = importer._construct_raw_filepath_from_filepath(filepath)

    # THEN
    assert raw_filepath == raw_filepath_expected


@pytest.mark.unit
def test_set_raw_filepath_on_dataset(importer: FiftyoneImporter):

    # GIVEN
    mock_dataset = Mock()
    mock_dataset.has_sample_field.return_value = False

    # Create a list of mock samples for the dataset
    mock_samples = [Mock(filepath=f"s3://dev-proj-anonymized/samples/dataset/{idx}_anonymized.png",
                         expected_raw=f"s3://dev-proj-raw/samples/dataset/{idx}.png",
                         raw_filepath="",
                         set_field=MagicMock()) for idx in range(0, 5)]

    # Configure the mocked dataset to use the list of mock samples
    mock_dataset.select_fields.return_value = iter(mock_samples)

    # WHEN
    # Call the method with the mocked dataset
    importer.set_raw_filepath_on_dataset(mock_dataset)

    # THEN

    mock_dataset.has_sample_field.assert_called_with("raw_filepath")
    mock_dataset.add_sample_field.assert_called_with("raw_filepath", ftype=fo.StringField)

    for sample in mock_samples:

        sample.set_field.assert_called_with("raw_filepath", sample.expected_raw, dynamic=True)
        sample.save.assert_called_once()


@ pytest.mark.unit
def test_from_dir(fiftyone, importer: FiftyoneImporter):

    # GIVEN
    dataset = MagicMock()
    fiftyone.Dataset.from_dir.return_value = dataset
    mock_set_raw_filepath_on_dataset = MagicMock(return_value=dataset)
    importer.set_raw_filepath_on_dataset = mock_set_raw_filepath_on_dataset
    sample_kwargs = {"arg1": "value1", "arg2": "value2"}

    # WHEN
    result = importer.from_dir(**sample_kwargs)

    # THEN
    fiftyone.Dataset.from_dir.assert_called_with(dataset_type=fo.types.FiftyOneDataset, **sample_kwargs)
    mock_set_raw_filepath_on_dataset.assert_called_with(dataset)
    assert result == dataset
