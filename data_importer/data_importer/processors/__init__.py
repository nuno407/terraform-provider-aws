"""Processors module"""


def load_all_processors():
    """
    Load all relevant Processors.
    New Processors need to be added here so they will be picked up by the ProcessorRepository
    """
    # pylint: disable=import-outside-toplevel,unused-import,cyclic-import
    from data_importer.processors.image_processor import ImageMetadataLoader
    from data_importer.processors.metadata_processor import JsonMetadataLoader
    from data_importer.processors.zip_dataset_processor import ZipDatasetProcessor
