"""Fiftyone Importer module"""
from typing import Any, Optional

import fiftyone as fo
from fiftyone import ViewField as F

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

    def _find_or_create_sample(self, dataset: fo.Dataset, path: str):
        try:
            sample = self.find_sample(dataset, path)
            _logger.debug("Found sample with path %s", path)
        except ValueError:
            sample = fo.Sample(path)
            dataset.add_sample(sample)
            _logger.debug("Created sample with path %s", path)
        return sample

    def replace_sample(self, dataset: fo.Dataset, path: str, metadata: Optional[dict[Any, Any]] = None):
        """
        Replaces a sample by the given file path by either finding the existing one
        or creating a new one if it doesn't exist yet.
        If metadata is provided it will override the Sample's metadata with the new one.
        Fields internal to the Sample are not changed (e.g. ID and filepath).

        :param dataset: Dataset to use
        :param path: Filepath of the sample
        :param metadata: Optional metadata to set on the Sample
        :return: the Sample
        """

        sample = self._find_or_create_sample(dataset, path)

        if metadata is not None:
            self.override_metadata(sample, metadata)
        sample.save()
        return sample

    def upsert_sample(self, dataset: fo.Dataset, path: str, metadata: dict[Any, Any]):
        """
        Updates a sample by the given file path by either finding the existing one
        or creating a new one if it doesn't exist yet.
        If metadata is provided it will override the Sample's metadata with the new one.

        :param dataset: Dataset to use
        :param path: Filepath of the sample
        :param metadata: Optional metadata to set on the Sample
        :return: the Sample
        """

        sample = self._find_or_create_sample(dataset, path)

        for key, value in metadata.items():
            _logger.info("Setting key %s value %s", key, value)
            self._set_field(sample, key, value)

        sample.save()
        return sample

    def delete_sample(self, dataset: fo.Dataset, path: str):
        """
        Deletes the sample with the given file path. Does nothing when the sample can't be found.
        :param dataset: Dataset to delete the sample from
        :param path: Filepath of the sample to delete
        """
        try:
            sample_to_delete = self.find_sample(dataset, path)
            dataset.delete_samples([sample_to_delete])
        except ValueError:
            _logger.warning("Sample not found, will skip deletion.")

    def find_sample(self, dataset: fo.Dataset, path: str):
        """
        Finds a sample based on the path without extension.

        :param dataset: Dataset to find the sample in
        :param path: Filepath of the sample to find
        :return: The sample
        :raises ValueError when the sample couldn't be found
        """
        filename = path.split(".")[0]
        return dataset.one(F("filepath").starts_with(filename))

    def override_metadata(self, sample: fo.Sample, metadata: dict):
        """
        Overrides the metadata of the given sample with the metadata in the given dict

        :param sample: Sample to override the metadata from
        :param metadata: New metadata to use for the sample
        """
        self.delete_metadata(sample)
        for key, value in metadata.items():
            self._set_field(sample, key, value)

    def delete_metadata(self, sample: fo.Sample):
        """
        Deletes all non default metadata from the sample

        :param sample: Sample to delete metadata from
        """
        metadata_to_remove = filter(lambda metadata: metadata[0] not in self.default_sample_fields,
                                    sample.iter_fields())
        for key, _ in metadata_to_remove:
            sample.clear_field(key)

    def _set_field(self, sample: fo.Sample, key, value):
        """
        Set field in voxel sample
        -> if value is a dict sets it as DynamicEmbddedDocument so it can be visualized in the UI

        Args:
            sample : fiftyone.sample
            key : field name
            value : field value
        """
        if isinstance(value, dict):
            sample.set_field(key, fo.DynamicEmbeddedDocument(**value))
        else:
            sample.set_field(key, value)

    def from_dir(self, **kwargs):
        """ Imports a fiftyone dataset from a local directory. """
        return fo.Dataset.from_dir(dataset_type=fo.types.FiftyOneDataset, **kwargs)
