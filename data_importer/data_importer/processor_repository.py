from data_importer.processor import Processor
from data_importer.processors import load_all_processors
from data_importer.processors.default_processor import DefaultProcessor


class ProcessorRepository():
    _repository: dict[str, Processor] = {}
    _default_processor = DefaultProcessor()

    @classmethod
    def register(cls, file_extensions: list[str]):
        def decorator(registered_class):
            for file_extension in file_extensions:
                cls._repository[file_extension] = registered_class
            return registered_class
        return decorator

    @classmethod
    def get_processor(cls, file_extension) -> Processor:
        return cls._repository.get(file_extension, cls._default_processor)

    @classmethod
    def load_all_processors(cls):
        load_all_processors()
