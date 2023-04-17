import fiftyone as fo
from fiftyone import ViewField as F
import logging
from datetime import datetime
from metadata.consumer.config import DatasetMappingConfig
from metadata.consumer.voxel.metadata_loader import VoxelMetadataLoader
from kink import inject
from mypy_boto3_s3 import S3Client
from base.aws.container_services import ContainerServices
from base.constants import IMAGE_FORMATS
from base.aws.s3 import S3Controller
import json
_logger = logging.getLogger(__name__)


@inject
def add_voxel_snapshot_metadata(snapshot_id: str, snapshot_path: str, metadata_path: str, s3_client: S3Client):
    _logger.info("Loading snapshot metadata from %s", metadata_path)

    # Load metadata file
    metadata_bucket, metadata_key = S3Controller.get_s3_path_parts(metadata_path)

    if not ContainerServices.check_s3_file_exists(s3_client, metadata_bucket, metadata_key):
        raise ValueError(f"Snapshot metadata {metadata_path} does not exist")

    # Load dataset name
    dataset_name, _ = _determine_dataset_name(snapshot_path)    # pylint: disable=no-value-for-parameter

    _logger.info("Searching for snapshot sample with s3_path=%s in dataset=%s", snapshot_path, dataset_name)
    dataset = fo.load_dataset(dataset_name)
    sample = dataset.one(F("video_id") == snapshot_id)

    raw_snapshot_metadata = ContainerServices.download_file(s3_client, metadata_bucket, metadata_key)
    metadata = json.loads(raw_snapshot_metadata.decode("UTF-8"))

    VoxelMetadataLoader.load_snapshot_metadata(sample, metadata)
    sample.save()


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
    _logger.info("Voxel sample has been saved correctly")


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


def create_dataset(dataset_name, tags) -> fo.Dataset:
    """
    Creates a voxel dataset with the given name or loads it if it already exists.

    Args:
        dataset_name: Dataset name to load or create
        tags: Tags to add on dataset creation
    """
    if fo.dataset_exists(dataset_name):
        return fo.load_dataset(dataset_name)
    else:
        dataset = fo.Dataset(dataset_name, persistent=True)
        if tags is not None:
            dataset.tags = tags
        _logger.info("Voxel dataset [%s] created!", dataset_name)
        return dataset


@inject
def _determine_dataset_name(filepath: str, mapping_config: DatasetMappingConfig):
    """
    Checks in config if tenant gets its own dataset or if it is part of the default dataset.
    Dedicated dataset names are prefixed with the tag given in the config.
    The tag is not added to the default dataset.

    :param filepath: S3 filepath to extract the tenant from
    :param mapping_config: Config with mapping information about the tenants
    :return: the resulting dataset name and the tags which should be added on dataset creation
    """
    _, path = S3Controller.get_s3_path_parts(filepath)
    s3split = path.split("/")
    # The root dir on the S3 bucket always is the tenant name
    tenant_name = s3split[0]
    filetype = s3split[-1].split(".")[-1]

    dataset_name = mapping_config.default_dataset
    tags = [mapping_config.tag]

    if tenant_name in mapping_config.create_dataset_for:
        dataset_name = f"{mapping_config.tag}-{tenant_name}"

    if filetype in IMAGE_FORMATS:
        dataset_name = dataset_name + "_snapshots"

    return dataset_name, tags


def update_on_voxel(filepath: str, sample: dict):
    """
    Updates a sample in a dataset with the given metadata. If the sample or dataset do not exist they will be created.
    The dataset name is derived by the given S3 file path.
    From the path the tenant is derived and the dataset determined.
    :param filepath: File path to extract the dataset information from.
    :param sample: Sample data to update. Uses `video_id` to find the sample.
    """

    dataset_name, tags = _determine_dataset_name(filepath)  # pylint: disable=no-value-for-parameter
    _logger.debug("Updating voxel path(%s) in dataset(%s)", filepath, dataset_name)
    try:
        create_dataset(dataset_name, tags)
        update_sample(dataset_name, sample)
    except Exception as err:  # pylint: disable=broad-except
        _logger.exception("Unable to process Voxel entry [%s] with %s", dataset_name, err)
