"""Class that inherits base metadata, to be extened in case we need several PreviewMetadata versions"""

from base.model.metadata.base_metadata import BaseMetadata


class PreviewMetadata(BaseMetadata):  # pylint: disable=abstract-method
    """Class to inherit base metadata"""
