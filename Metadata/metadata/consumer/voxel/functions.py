import fiftyone as fo
from fiftyone import ViewField
import logging
from datetime import datetime
from base.model.config.policy_config import PolicyConfig
from base.voxel.voxel_snapshot_metadata_loader import VoxelSnapshotMetadataLoader
from base.model.metadata_artifacts import Frame
from metadata.consumer.voxel.metadata_parser import MetadataParser
from kink import inject
from base.voxel.functions import create_dataset, get_anonymized_path_from_raw
from base.voxel.utils import determine_dataset_name, determine_dataset_by_path
from base.aws.s3 import S3Controller
from base.model.artifacts import SignalsArtifact
from metadata.consumer.exceptions import SnapshotNotFound
import json

_logger = logging.getLogger(__name__)


def _calculate_sample_tenant(filepath: str) -> str:
    """ Obtain tenant by filepath """
    _, file_key = S3Controller.get_s3_path_parts(filepath)
    s3split = file_key.split("/")

    tenant_name = s3split[0]
    return tenant_name


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
    dataset_name, _ = determine_dataset_name(tenant, True)  # pylint: disable=no-value-for-parameter
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
def get_voxel_sample_data_privacy_document_id(sample: fo.Sample, mapping_config: PolicyConfig) -> str:
    """
    Checks in config the associated data privacy document for the tenant
    """
    tenant = _calculate_sample_tenant(sample.filepath)
    if tenant in mapping_config.policy_document_per_tenant:
        return mapping_config.policy_document_per_tenant[tenant]
    return mapping_config.default_policy_document


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
    _logger.debug("Loading snapshot metadata from %s", metadata_artifact.s3_path)
    # Prepare s3 paths
    metadata_bucket, metadata_key = s3_controller.get_s3_path_parts(metadata_artifact.s3_path)

    # Load sample
    sample: fo.Sample = get_voxel_snapshot_sample(metadata_artifact.tenant_id,
                                                  metadata_artifact.referred_artifact.artifact_id)
    # Check if the metadata file exists
    if not s3_controller.check_s3_file_exists(metadata_bucket, metadata_key):
        raise ValueError(f"Snapshot metadata {metadata_artifact.referred_artifact.artifact_id} does not exist")

    # Download and convert metadata file
    raw_snapshot_metadata = s3_controller.download_file(metadata_bucket, metadata_key)
    metadata = json.loads(raw_snapshot_metadata.decode("UTF-8"))

    # Parse and check the number of frames of metadata
    metadata_frames: list[Frame] = metadata_parser.parse(metadata)

    if len(metadata_frames) == 0:
        _logger.warning("The snapshot's metadata is empty, nothing to ingest")
        return

    elif len(metadata_frames) > 1:
        raise ValueError("The snapshot's metadata contains more then one frame data.")

    voxel_loader.set_sample(sample)
    voxel_loader.load(metadata_frames[0])

    sample.save()
    _logger.info("Snapshot metadata has been saved to voxel")


def update_sample(dataset_name, sample_info: dict):
    """
    Creates a voxel sample (entry inside the provided dataset).

    Args:
        dataset_name: Dataset to add the sample to; needs to exist but is automatically loaded
        sample_info: Information to add to the Voxel sample
    """
    _logger.debug("sample_info: %s", sample_info)
    dataset = fo.load_dataset(dataset_name)
    sample_info.pop("filepath", None)
    # We do not want to propagate upload_rules mongodb entry into voxel
    # since there is already a "rules" field for it
    sample_info.pop("upload_rules", None)
    to_add = dict()

    # get the sample, or create one if it doesn't exist
    try:
        sample = dataset.one(ViewField("video_id") == sample_info["video_id"])
    except ValueError:
        sample = fo.Sample(filepath=sample_info["s3_path"])
        sample["data_privacy_document_id"] = get_voxel_sample_data_privacy_document_id(sample)
        dataset.add_sample(sample)
        _logger.debug("Voxel sample %s created", sample_info["s3_path"])

    # for fields at the root of sample_info
    for (key, val) in sample_info.items():
        if key == "algorithms":
            continue
        elif key.startswith("_") or key.startswith("filepath"):
            key = f"ivs{key}"
        to_add.update({key: val})

    # for fields nested inside recording_overview
    if recording_overview := sample_info.get("recording_overview", dict()):
        # put all of recording_overview as primitives
        for (key, val) in recording_overview.items():
            key = key if not key.startswith("_") else f"ivs{key}"
            sample.update_fields(fields_dict={key: val}, expand_schema=True)

        # unfold 'time' to simpler fields for querying as primitives
        if "time" in recording_overview:
            time_as_datetime = datetime.strptime(sample_info["recording_overview"]["time"], "%Y-%m-%d %H:%M:%S")
            to_add.update({
                "recording_time": time_as_datetime,
                "Hour": time_as_datetime.strftime("%H"),
                "Day": time_as_datetime.strftime("%d"),
                "Month": time_as_datetime.strftime("%b"),
                "Year": time_as_datetime.strftime("%Y"),
            })
    else:
        _logger.info("No items in recording overview: %s", sample_info.get("recording_overview"))

    sample.update_fields(fields_dict=to_add, expand_schema=True)

    # compute Voxel metadata fields for a sample
    try:
        sample.compute_metadata()
    except Exception as e:
        _logger.debug("Call to compute_metadata() raised an exception: %s", e)

    _logger.debug("Voxel sample to be saved as: %s", sample.to_dict())

    # Store sample on database
    sample.save()
    # Update dataset level schema
    dataset.add_dynamic_sample_fields(fields=to_add.keys())
    _logger.info("Voxel sample has been saved correctly")


def update_on_voxel(filepath: str, sample: dict):
    """
    Updates a sample in a dataset with the given metadata. If the sample or dataset do not exist they will be created.
    The dataset name is derived by the given S3 file path.

    Args:
        filepath: File path to extract the dataset information from.
        sample: Sample data to update. Uses `video_id` to find the sample.
    """
    dataset_name, tags = determine_dataset_by_path(filepath)  # pylint: disable=no-value-for-parameter
    _logger.debug("Updating voxel sample %s in dataset %s", filepath, dataset_name)
    create_dataset(dataset_name, tags)
    update_sample(dataset_name, sample)


def update_rule_on_voxel(raw_filepath: str, video_id: str, tenant: str, rule: dict):
    """Updates a sample on Voxel with the rule that triggered the upload.
    If the samples does not exist, is going to create a new one.

    Args:
        filepaht (str): raw file path
        rule (dict): Fields to upload on voxel
    """
    filepath = get_anonymized_path_from_raw(raw_filepath)
    dataset_name, tags = determine_dataset_by_path(filepath)  # pylint: disable=no-value-for-parameter
    _logger.debug("Updating voxel sample %s in dataset %s", filepath, dataset_name)
    create_dataset(dataset_name, tags)

    _logger.debug("sample_rules_info: %s", rule)
    dataset = fo.load_dataset(dataset_name)
    try:
        if filepath not in dataset:
            sample = fo.Sample(filepath=filepath)
            dataset.add_sample(sample)
            sample["video_id"] = video_id
            sample["tenantID"] = tenant
            sample["raw_filepath"] = raw_filepath
            sample.save()

        sample = dataset.one(ViewField("filepath") == filepath)
        doc = fo.DynamicEmbeddedDocument(**rule)

        if "rules" not in sample or sample["rules"] is None:
            sample["rules"] = [doc]
        else:
            current_list_value_in_dict_format = [value.to_json() for value in sample["rules"]]
            current_list_value_in_dict_format.extend(doc.to_json())
            unique_values = list(set(current_list_value_in_dict_format))
            sample["rules"] = [fo.DynamicEmbeddedDocument.from_json(unique_value) for unique_value in unique_values]
        sample.save()
        dataset.add_dynamic_sample_fields()

    except ValueError:
        _logger.debug("Failed to create sample %s in dataset %s", filepath, dataset_name)
