""" Module containing all the voxel operations. """
import logging
from datetime import datetime

import fiftyone as fo  # type: ignore
from fiftyone import ViewField as F  # type: ignore

_logger = logging.getLogger(__name__)


def create_dataset(dataset_name, tags):
    """
    Creates a voxel dataset with the given name or loads it if it already exists.

    Args:
        dataset_name: Dataset name to load or create
        tags: Tags to add on dataset creation
    """
    if fo.dataset_exists(dataset_name):
        fo.load_dataset(dataset_name)
    else:
        dataset = fo.Dataset(dataset_name, persistent=True)
        if tags is not None:
            dataset.tags = tags
        _logger.info("Voxel dataset [%s] created!", dataset_name)


def update_sample(data_set, sample_info):
    """
    Creates a voxel sample (entry inside the provided dataset).

    Args:
        data_set: Dataset to add the sample to; needs to exist but is automatically loaded
        sample_info: Metadata to add to the sample
    """

    dataset = fo.load_dataset(data_set)

    # If the sample already exists, update its information, otherwise create a new one
    if "filepath" in sample_info:
        sample_info.pop("filepath")

    try:
        sample = dataset.one(F("video_id") == sample_info["video_id"])
    except ValueError:
        sample = fo.Sample(filepath=sample_info["s3_path"])
        dataset.add_sample(sample)
        _logger.debug("Voxel sample [%s] created!", sample_info["s3_path"])

    _logger.debug("sample_info: %s !", sample_info)

    for (key, value) in sample_info.items():
        if key == "algorithms":
            continue
        if key.startswith("_") or key.startswith("filepath"):
            key = "ivs" + key
        sample[key] = value

    _populate_metadata(sample, sample_info)

    # Store sample on database
    sample.save()


def _populate_metadata(sample: fo.Sample, sample_info):
    # Parse and populate labels and metadata on sample
    if "recording_overview" in sample_info:
        for (key, value) in sample_info.get("recording_overview").items():
            if key.startswith("_"):
                key = "ivs" + key
            try:
                _logger.debug("Adding (key: value) '%s': '%s' to voxel sample", str(key), value)
                sample[str(key)] = value
            except Exception as exp:  # pylint: disable=broad-except
                _logger.exception("sample[%s] = %s, %s", str(key), value, str(type(value)))
                _logger.exception("%s", str(exp))

        if "time" in sample["recording_overview"]:
            time = sample["recording_overview"]["time"]
            sample["recording_time"] = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
            sample["Hour"] = sample["recording_time"].strftime("%H")
            sample["Day"] = sample["recording_time"].strftime("%d")
            sample["Month"] = sample["recording_time"].strftime("%b")
            sample["Year"] = sample["recording_time"].strftime("%Y")
            _logger.info(sample["recording_time"])
        else:
            _logger.info("No time")
    else:
        _logger.info("No items in recording overview")
        _logger.info(sample_info.get("recording_overview"))
