"""Processor Repository Module"""
from data_importer.processor import Processor
from data_importer.processors import load_all_processors
from data_importer.processors.default_processor import DefaultProcessor


class ProcessorRepository():
    """Repository provides processors to handle metadata based on the file type"""

    _repository: dict[str, Processor] = {}
    _default_processor = DefaultProcessor()

    @classmethod
    def register(cls, file_extensions: list[str]):
        """
        Register a Processor
        :param file_extensions: File extensions this Processor accepts
        """
        def decorator(registered_class):
            for file_extension in file_extensions:
                cls._repository[file_extension] = registered_class
            return registered_class
        return decorator

    @classmethod
    def get_processor(cls, file_extension) -> Processor:
        """
        Get the Processor for the given file extension
        :param file_extension: The file extension
        :return: The Processor for this file extension or the DefaultProcessor if no suitable was found
        """
        return cls._repository.get(file_extension, cls._default_processor)

    @classmethod
    def load_all_processors(cls):
        """
        Automatically load all Processors
        """
        load_all_processors()
