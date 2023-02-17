import fiftyone as fo
from base.aws.container_services import ContainerServices
from fiftyone import ViewField as F

_logger = ContainerServices.configure_logging(__name__)


class FiftyoneImporter:
    """
    Voxel Fiftyone importer for media and metadata has methods to find, create, and delete samples based on the file path
    """

    default_sample_fields = [key for key, _ in fo.Sample("blueprint").iter_fields()]

    def load_dataset(self, name: str, tags: [str]):
        """
        Loads an existing dataset or creates a new one if it doesn't exist yet.

        :param name: Dataset name
        :param tags: Tags added when creating the dataset
        :return: fiftyone.Dataset
        """
        if fo.dataset_exists(name):
            return fo.load_dataset(name)
        dataset = fo.Dataset(name, persistent=True, overwrite=False)
        dataset.tags = tags
        return dataset

    def upsert_sample(self, dataset: fo.Dataset, path: str, metadata: dict = None):
        """
        Upserts a sample by the given file path by either finding the existing one
        or creating a new one if it doesn't exist yet.
        If metadata is provided it will override the Sample's metadata with the new one.

        :param dataset: Dataset to use
        :param path: Filepath of the sample
        :param metadata: Optional metadata to set on the Sample
        :return: the Sample
        """
        try:
            sample = self.find_sample(dataset, path)
            _logger.debug("Found sample with path %s", path)
        except ValueError:
            sample = fo.Sample(path)
            dataset.add_sample(sample)
            _logger.debug("Created sample with path %s", path)

        if metadata is not None:
            self.override_metadata(sample, metadata)
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
        [sample.set_field(key, value) for key, value in metadata.items()]

    def delete_metadata(self, sample: fo.Sample):
        """
        Deletes all non default metadata from the sample

        :param sample: Sample to delete metadata from
        """
        metadata_to_remove = filter(
            lambda metadata: metadata[0] not in self.default_sample_fields,
            sample.iter_fields())
        [sample.clear_field(key) for key, _ in metadata_to_remove]
