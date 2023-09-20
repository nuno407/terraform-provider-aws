from kink import inject

from base.aws.s3 import S3Controller
from base.constants import IMAGE_FORMATS

from base.model.config.dataset_config import DatasetConfig


@inject
def determine_dataset_name(tenant: str, is_snapshot: bool, mapping_config: DatasetConfig) -> tuple[str, list[str]]:
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

    for dataset in mapping_config.create_dataset_for:
        if tenant in dataset.tenants:
            dataset_name = f"{mapping_config.tag}-{dataset.name}"
            break

    if is_snapshot:
        dataset_name = dataset_name + "_snapshots"

    return dataset_name, tags


@inject
def determine_dataset_by_path(filepath: str, mapping_config: DatasetConfig) -> tuple[str, list[str]]:
    """
    Method to maintain compatability with legacy functions that gets the tenant by splitting the path.

    :param filepath: S3 filepath to extract the tenant from
    :param mapping_config: Config with mapping information about the tenants
    :return: the resulting dataset name and the tags which should be added on dataset creation
    """
    _, file_key = S3Controller.get_s3_path_parts(filepath)
    s3split = file_key.split("/")

    tenant_name = s3split[0]
    filetype = s3split[-1].split(".")[-1]

    is_snapshot = filetype in IMAGE_FORMATS
    return determine_dataset_name(tenant_name, is_snapshot, mapping_config)  # type: ignore
