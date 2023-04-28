import fiftyone as fo
from fiftyone import ViewField
import logging
from datetime import datetime
from metadata.consumer.config import DatasetMappingConfig
from base.voxel.voxel_snapshot_metadata_loader import VoxelSnapshotMetadataLoader
from base.model.metadata_artifacts import Frame
from metadata.consumer.voxel.metadata_parser import MetadataParser
from kink import inject
from base.voxel.constants import POSE_LABEL, VOXEL_KEYPOINTS_LABELS, VOXEL_SKELETON_LIMBS
from base.voxel.functions import create_dataset
from base.constants import IMAGE_FORMATS
from base.aws.s3 import S3Controller
import json
from typing import Any
_logger = logging.getLogger(__name__)


def get_voxel_sample(img_key: str, snapshot_id: str) -> fo.Sample:
    """
    Returns a voxel sample.
    This functions expects that the sample ALWAYS exists, if the same does not
    exist in voxel, an exception will be thrown.

    Args:
        img_key (str): The image key to the S3
        snapshot_id (str): The snapshot ID

    Returns:
        fo.Sample: The sample in voxel.
    """
    dataset_name, _ = _determine_dataset_name(
        img_key)    # pylint: disable=no-value-for-parameter
    _logger.info("Searching for snapshot sample with id=%s in dataset=%s",
                 snapshot_id, dataset_name)
    dataset = fo.load_dataset(dataset_name)
    return dataset.one(ViewField("video_id") == snapshot_id)


@inject
def add_voxel_snapshot_metadata(
        snapshot_id: str,
        snapshot_path: str,
        metadata_path: str,
        s3_controller: S3Controller,
        metadata_parser: MetadataParser,
        voxel_snapshot_loader: VoxelSnapshotMetadataLoader):
    """
    Downloads the metadata form S3, parses and uploads it to voxel.

    Args:
        snapshot_id (str): The snapshot ID
        snapshot_path (str): The snapshot path (with s3://)
        metadata_path (str):  The metadata path (with s3://)
        s3_client (S3Controller): An S3 Controler to download from S3

    Raises:
        ValueError: If a metadata file was not found or there was some problem on the ingestion
    """
    _logger.debug("Loading snapshot metadata from %s", metadata_path)
    # Prepare s3 paths
    metadata_bucket, metadata_key = s3_controller.get_s3_path_parts(
        metadata_path)
    _, img_key = s3_controller.get_s3_path_parts(snapshot_path)

    # Load sample
    sample: fo.Sample = get_voxel_sample(img_key, snapshot_id)
    # Check if the metadata file exists
    if not s3_controller.check_s3_file_exists(metadata_bucket, metadata_key):
        print("TEST")
        raise ValueError(f"Snapshot metadata {metadata_path} does not exist")

    # Download and convert metadata file
    raw_snapshot_metadata = s3_controller.download_file(
        metadata_bucket, metadata_key)
    metadata = json.loads(raw_snapshot_metadata.decode("UTF-8"))

    # Parse and check the number of frames of metadata
    metadata_frames: list[Frame] = metadata_parser.parse(metadata)

    if len(metadata_frames) == 0:
        _logger.warning("The snapshot's metadata is empty")
        return

    elif len(metadata_frames) > 1:
        raise ValueError(
            "The snapshot's metadata contains more then one frame data.")

    voxel_snapshot_loader.set_sample(sample)
    voxel_snapshot_loader.load(metadata_frames[0])

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
        sample = dataset.one(ViewField("video_id") == sample_info["video_id"])
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


def _populate_metadata(sample: fo.Sample, sample_info: dict[Any, Any]):
    """
    Populates metadata on the voxel smaple.
    The caller is responsible for saving the sample.

    Args:
        sample (fo.Sample): The sample to update the metadata.
        sample_info (dict): _description_
    """
    if "recording_overview" not in sample_info:
        _logger.info("No items in recording overview")
        _logger.info(sample_info.get("recording_overview"))
        return

    for (key, value) in sample_info.get("recording_overview").items():
        if key.startswith("_"):
            key = "ivs" + key
            _logger.debug(
                "Adding (key: value) '%s': '%s' to voxel sample", str(key), value)
            sample[str(key)] = value

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


def __set_dataset_skeleton_configuration(dataset: fo.Dataset) -> None:
    dataset.skeletons = {
        POSE_LABEL: fo.KeypointSkeleton(
            labels=VOXEL_KEYPOINTS_LABELS,
            edges=VOXEL_SKELETON_LIMBS,
        )
    }


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
    s3split = filepath.split("/")
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

    dataset_name, tags = _determine_dataset_name(
        filepath)  # pylint: disable=no-value-for-parameter
    _logger.debug("Updating voxel path(%s) in dataset(%s)",
                  filepath, dataset_name)
    create_dataset(dataset_name, tags)
    update_sample(dataset_name, sample)
