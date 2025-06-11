from ..interfaces.file_reader import FileReaderInterface
from .csv_reader import CSVReader
from .json_reader import JSONReader

class FileReaderFactory:
    _readers = {}

    @classmethod
    def register_reader(cls, extension: str, reader_cls):
        cls._readers[extension.lower()] = reader_cls

    @classmethod
    def get_reader(cls, filepath: str) -> FileReaderInterface:
        ext = filepath.split('.')[-1].lower()
        reader_cls = cls._readers.get(ext)
        if not reader_cls:
            raise ValueError(f"Formato de archivo '{ext}' no soportado")
        return reader_cls()


# Register the readers
FileReaderFactory.register_reader('csv', CSVReader)
FileReaderFactory.register_reader('json', JSONReader)