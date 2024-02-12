import re
from base.model.config.policy_config import PolicyConfig
from base.voxel.constants import OPENLABEL_KEYPOINTS_LABELS, VOXEL_KEYPOINTS_LABELS, VOXEL_SKELETON_LIMBS, POSE_LABEL, GT_POSE_LABEL, INFERENCE_POSE
import fiftyone as fo
from fiftyone import ViewField as F
import logging
from kink import inject
from base.aws.s3 import S3Controller

_logger = logging.getLogger(__name__)


def set_dataset_skeleton_configuration(dataset: fo.Dataset) -> None:
    skeleton = fo.KeypointSkeleton(
        labels=VOXEL_KEYPOINTS_LABELS,
        edges=VOXEL_SKELETON_LIMBS,
    )
    dataset.skeletons = {
        POSE_LABEL: skeleton,
        GT_POSE_LABEL: skeleton,
        INFERENCE_POSE: skeleton
    }


def openlabel_skeleton() -> fo.KeypointSkeleton:
    return fo.KeypointSkeleton(
        labels=OPENLABEL_KEYPOINTS_LABELS,
        edges=VOXEL_SKELETON_LIMBS
    )


def _set_mandatory_data_fields(dataset: fo.Dataset) -> fo.Dataset:
    """
    Create all mandantory fields on the dataset level
    """

    if not dataset.has_sample_field("raw_filepath"):
        dataset.add_sample_field("raw_filepath", ftype=fo.StringField)
    if not dataset.has_sample_field("data_privacy_document_id"):
        dataset.add_sample_field("data_privacy_document_id", ftype=fo.StringField)
    return dataset


def create_dataset(dataset_name, tags) -> fo.Dataset:
    """
    Creates a voxel dataset with the given name or loads it if it already exists.

    Args:
        dataset_name: Dataset name to load or create
        tags: Tags to add on dataset creation
    """
    if fo.dataset_exists(dataset_name):
        _logger.debug("Loading dataset [%s]", dataset_name)
        return fo.load_dataset(dataset_name)
    else:
        dataset = fo.Dataset(dataset_name, persistent=True)
        set_dataset_skeleton_configuration(dataset)
        _set_mandatory_data_fields(dataset)
        if tags is not None:
            dataset.tags = tags
        _logger.info("Voxel dataset [%s] created!", dataset_name)
        return dataset


def find_sample(dataset: fo.Dataset, path: str):
    """
    Finds a sample based on the path without extension.

    :param dataset: Dataset to find the sample in
    :param path: Filepath of the sample to find
    :return: The sample
    :raises ValueError when the sample couldn't be found
    """
    filename = path.split(".")[0]
    return dataset.one(F("filepath").starts_with(filename))


def find_or_create_sample(dataset: fo.Dataset, path: str):
    try:
        sample = find_sample(dataset, path)
        _logger.debug("Found sample with path %s", path)
    except ValueError:
        sample = fo.Sample(path)
        dataset.add_sample(sample, dynamic=True)
        _logger.debug("Created sample with path %s", path)
    return sample


def set_field(sample: fo.Sample, key, value):
    """
    Set field in voxel sample
    -> if value is a dict sets it as DynamicEmbddedDocument so it can be visualized in the UI

    Args:
        sample : fiftyone.sample
        key : field name
        value : field value
    """
    if isinstance(value, dict):
        sample.set_field(key, fo.DynamicEmbeddedDocument(**value), dynamic=True)
    else:
        sample.set_field(key, value, dynamic=True)


def delete_sample(dataset: fo.Dataset, path: str):
    """
    Deletes the sample with the given file path. Does nothing when the sample can't be found.
    :param dataset: Dataset to delete the sample from
    :param path: Filepath of the sample to delete
    """
    try:
        sample_to_delete = find_sample(dataset, path)
        dataset.delete_samples([sample_to_delete])
    except ValueError:
        _logger.warning("Sample not found, will skip deletion.")


def _construct_raw_filepath_from_filepath(filepath: str) -> str:
    """ Replace anonymized in bucket name with raw and remove anonymized suffix from file name """

    # replace anonymized with raw in bucket name only
    bucket_pattern = r"^(s3://.*-.*-)(anonymized)(.*)"
    replacement = r"\g<1>raw\g<3>"
    raw_filepath = re.sub(bucket_pattern, replacement, filepath)

    # remove file suffix
    suffix_pattern = r"(.*)_anonymized(\.\w+)$"
    replacement = r"\g<1>\g<2>"
    raw_filepath = re.sub(suffix_pattern, replacement, raw_filepath)

    return raw_filepath


def get_anonymized_path_from_raw(filepath: str) -> str:
    bucket, path = S3Controller.get_s3_path_parts(filepath)
    anon_bucket = bucket.replace("raw", "anonymized")

    file_path_no_extension, extension = path.split(".")

    return f"s3://{anon_bucket}/{file_path_no_extension}_anonymized.{extension}"


@inject()
def set_policy_document_id(sample: fo.Sample, tenant_id: str, config: PolicyConfig):
    """
    Set data_privacy_document_id on a sample
    """
    if sample.has_field("data_privacy_document_id") and sample.get_field("data_privacy_document_id") is not None:
        return

    value = config.default_policy_document
    if tenant_id in config.policy_document_per_tenant:
        value = config.policy_document_per_tenant[tenant_id]

    set_field(
        sample=sample,
        key="data_privacy_document_id",
        value=value)


def set_raw_filepath_on_sample(sample: fo.Sample):
    """
    Set raw_filepath on a sample based on filepath replacing 'anonymized' with 'raw'
    """
    if sample.has_field("raw_filepath") and sample.get_field("raw_filepath") is not None:
        return

    raw_filepath = _construct_raw_filepath_from_filepath(sample.filepath)

    set_field(
        sample=sample,
        key="raw_filepath",
        value=raw_filepath)


def set_mandatory_fields_on_sample(sample: fo.Sample, tenant_id: str):
    """
    Set mandatory fields on the specified sample
    """
    set_raw_filepath_on_sample(sample)
    set_policy_document_id(sample=sample, tenant_id=tenant_id)
