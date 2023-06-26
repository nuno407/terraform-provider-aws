import fiftyone as fo
from fiftyone import ViewField
import logging
from datetime import datetime
from metadata.consumer.config import DatasetMappingConfig
from base.voxel.voxel_snapshot_metadata_loader import VoxelSnapshotMetadataLoader
from base.model.metadata_artifacts import Frame
from metadata.consumer.voxel.metadata_parser import MetadataParser
from kink import inject
from base.voxel.functions import create_dataset
from base.constants import IMAGE_FORMATS
from base.aws.s3 import S3Controller
from base.model.artifacts import SignalsArtifact
from metadata.consumer.exceptions import SnapshotNotFound
import json
from typing import Any
_logger = logging.getLogger(__name__)


def get_voxel_snapshot_sample(tenant: str, snapshot_id: str) -> fo.Sample:
    """
    Returns a voxel sample.
    This functions expects that the sample ALWAYS exists, if the same does not
    exist in voxel, an exception will be thrown.

    Args:
        tenant (str): The tenant name
        snapshot_id (str): The snapshot ID

    Returns:
        fo.Sample: The sample in voxel.
    """
    dataset_name, _ = _determine_dataset_name(
        tenant, True)    # pylint: disable=no-value-for-parameter
    _logger.info("Searching for snapshot sample with id=%s in dataset=%s",
                 snapshot_id, dataset_name)
    dataset = fo.load_dataset(dataset_name)

    try:
        sample = dataset.one(ViewField("video_id") == snapshot_id)
    except ValueError as exce:
        raise SnapshotNotFound(
            f"The snapshot with id={snapshot_id} was not found") from exce

    return sample


@inject
def add_voxel_snapshot_metadata(
        metadata_artifact: SignalsArtifact,
        s3_controller: S3Controller,
        metadata_parser: MetadataParser,
        voxel_loader: VoxelSnapshotMetadataLoader):
    """
    Downloads the metadata form S3, parses and uploads it to voxel.

    Args:
        metadata_artifact (SignalsArtifact): The signals artifact
        s3_controller (S3Controller): The S3 controller to interact with S3
        metadata_parser (MetadataParser):  The parser for the metadata
        voxel_loader (VoxelSnapshotMetadataLoader): The Voxel Metadata snapshot loader

    Raises:
        ValueError: If a metadata file was not found or there was some problem on the ingestion
    """
    _logger.debug("Loading snapshot metadata from %s",
                  metadata_artifact.s3_path)
    # Prepare s3 paths
    metadata_bucket, metadata_key = s3_controller.get_s3_path_parts(
        metadata_artifact.s3_path)

    # Load sample
    sample: fo.Sample = get_voxel_snapshot_sample(
        metadata_artifact.tenant_id, metadata_artifact.referred_artifact.artifact_id)
    # Check if the metadata file exists
    if not s3_controller.check_s3_file_exists(metadata_bucket, metadata_key):
        raise ValueError(
            f"Snapshot metadata {metadata_artifact.referred_artifact.artifact_id} does not exist")

    # Download and convert metadata file
    raw_snapshot_metadata = s3_controller.download_file(
        metadata_bucket, metadata_key)
    metadata = json.loads(raw_snapshot_metadata.decode("UTF-8"))

    # Parse and check the number of frames of metadata
    metadata_frames: list[Frame] = metadata_parser.parse(metadata)

    if len(metadata_frames) == 0:
        _logger.warning("The snapshot's metadata is empty, nothing to ingest")
        return

    elif len(metadata_frames) > 1:
        raise ValueError(
            "The snapshot's metadata contains more then one frame data.")

    voxel_loader.set_sample(sample)
    voxel_loader.load(metadata_frames[0])

    sample.save()
    _logger.info("Snapshot metadata has been saved to voxel")


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

    #Compute Voxel metadata fields for a sample
    try:
        sample.compute_metadata()
    except Exception:
        _logger.debug("Failed to compute_metadata")

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


@inject
def _determine_dataset_name(tenant: str, is_snapshot: bool, mapping_config: DatasetMappingConfig) -> tuple[str, list[str]]:
    """
    Checks in config if tenant gets its own dataset or if it is part of the default dataset.
    Dedicated dataset names are prefixed with the tag given in the config.
    The tag is not added to the default dataset.

    :param tenant: The tenant to search for the dataset
    :param is_snapshot: A flag if the dataset is snapshot or not
    :param mapping_config: Config with mapping information about the tenants
    :return: the resulting dataset name and the tags which should be added on dataset creation
    """
    dataset_name = mapping_config.default_dataset
    tags = [mapping_config.tag]

    if tenant in mapping_config.create_dataset_for:
        dataset_name = f"{mapping_config.tag}-{tenant}"

    if is_snapshot:
        dataset_name = dataset_name + "_snapshots"

    return dataset_name, tags


@inject
def _determine_dataset_by_path(filepath: str, mapping_config: DatasetMappingConfig) -> tuple[str, list[str]]:
    """
    Method to mantain compatability with legacy functions that get's the tenant by spliting the path.

    :param filepath: S3 filepath to extract the tenant from
    :param mapping_config: Config with mapping information about the tenants
    :return: the resulting dataset name and the tags which should be added on dataset creation
    """
    _, file_key = S3Controller.get_s3_path_parts(filepath)
    s3split = file_key.split("/")

    tenant_name = s3split[0]
    filetype = s3split[-1].split(".")[-1]

    if filetype in IMAGE_FORMATS:
        return _determine_dataset_name(tenant_name, True, mapping_config)

    return _determine_dataset_name(tenant_name, False, mapping_config)


def update_on_voxel(filepath: str, sample: dict):
    """
    Updates a sample in a dataset with the given metadata. If the sample or dataset do not exist they will be created.
    The dataset name is derived by the given S3 file path.
    From the path the tenant is derived and the dataset determined.
    :param filepath: File path to extract the dataset information from.
    :param sample: Sample data to update. Uses `video_id` to find the sample.
    """

    dataset_name, tags = _determine_dataset_by_path(
        filepath)  # pylint: disable=no-value-for-parameter
    _logger.debug("Updating voxel path(%s) in dataset(%s)",
                  filepath, dataset_name)
    create_dataset(dataset_name, tags)
    update_sample(dataset_name, sample)
