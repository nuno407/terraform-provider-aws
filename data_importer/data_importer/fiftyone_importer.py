"""Fiftyone Importer module"""
from typing import Any, Optional
import fiftyone as fo

from base.voxel.functions import set_mandatory_fields_on_sample, set_field, \
    find_or_create_sample, set_raw_filepath_on_sample
from base.aws.container_services import ContainerServices

_logger = ContainerServices.configure_logging(__name__)


class FiftyoneImporter:
    """
    Voxel Fiftyone importer for media and metadata has methods to
    find, create, and delete samples based on the file path
    """

    default_sample_fields = [key for key, _ in fo.Sample("blueprint").iter_fields()]

    def check_if_dataset_exists(self, name: str):
        """ Checks if the dataset with the provided name exists. """
        return fo.dataset_exists(name)

    def load_dataset(self, name: str, tags: list[str]):
        """
        Loads an existing dataset or creates a new one if it doesn't exist yet.

        :param name: Dataset name
        :param tags: Tags added when creating the dataset
        :return: fiftyone.Dataset
        """
        if self.check_if_dataset_exists(name):
            return fo.load_dataset(name)
        dataset = fo.Dataset(name, persistent=True, overwrite=False)
        dataset.tags = tags
        return dataset

    def replace_sample(self, tenant_id: str, dataset: fo.Dataset, path: str, metadata: Optional[dict[Any, Any]] = None):
        """
        Replaces a sample by the given file path by either finding the existing one
        or creating a new one if it doesn't exist yet.
        If metadata is provided it will override the Sample's metadata with the new one.
        Fields internal to the Sample are not changed (e.g. ID and filepath).

        :param tenant_id: Tenant id
        :param dataset: Dataset to use
        :param path: Filepath of the sample
        :param metadata: Optional metadata to set on the Sample
        :return: the Sample
        """

        sample = find_or_create_sample(dataset, path)

        if metadata is not None:
            self.override_metadata(sample, metadata)

        set_mandatory_fields_on_sample(sample, tenant_id=tenant_id)
        sample.save()
        return sample

    def upsert_sample(self, tenant_id: str, dataset: fo.Dataset, path: str, metadata: dict[Any, Any]):
        """
        Updates a sample by the given file path by either finding the existing one
        or creating a new one if it doesn't exist yet.
        If metadata is provided it will override the Sample's metadata with the new one.

        :param tenant_id: Tenant id
        :param dataset: Dataset to use
        :param path: Filepath of the sample
        :param metadata: Optional metadata to set on the Sample
        :return: the Sample
        """

        sample = find_or_create_sample(dataset, path)

        for key, value in metadata.items():
            _logger.info("Setting key %s value %s", key, value)
            set_field(sample, key, value)

        set_mandatory_fields_on_sample(sample, tenant_id=tenant_id)
        sample.save()
        return sample

    def override_metadata(self, sample: fo.Sample, metadata: dict):
        """
        Overrides the metadata of the given sample with the metadata in the given dict

        :param sample: Sample to override the metadata from
        :param metadata: New metadata to use for the sample
        """
        self.delete_metadata(sample)
        for key, value in metadata.items():
            set_field(sample, key, value)

    def delete_metadata(self, sample: fo.Sample):
        """
        Deletes all non default metadata from the sample

        :param sample: Sample to delete metadata from
        """
        metadata_to_remove = filter(lambda metadata: metadata[0] not in self.default_sample_fields,
                                    sample.iter_fields())
        for key, _ in metadata_to_remove:
            sample.clear_field(key)

    def set_raw_filepath_on_dataset(self, dataset: fo.Dataset) -> fo.Dataset:
        """
        Create and set field 'raw_filepath' to raw path based on filepath replacing 'anonymized' with 'raw'
        """

        if not dataset.has_sample_field("raw_filepath"):
            dataset.add_sample_field("raw_filepath", ftype=fo.StringField)

        for sample in dataset.select_fields("raw_filepath", "filepath"):
            set_raw_filepath_on_sample(sample)
            sample.save()

        return dataset

    def from_dir(self, **kwargs):
        """ Imports a fiftyone dataset from a local directory. """

        dataset = fo.Dataset.from_dir(dataset_type=fo.types.FiftyOneDataset, **kwargs)

        # Ensure raw_filepath to facilitate use cases based on raw samples (e.g. labeling)
        dataset = self.set_raw_filepath_on_dataset(dataset)
        return dataset
