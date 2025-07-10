import os
from modules._common.domain.interfaces.file_reader import IFileReader
from modules.data_processing.application.schemas.preload_camp_schema import BaseFileConfig
from typing import Type
from .csv_reader import CSVReader
from .xlsx_reader import ExcelReader

class FileReaderFactory:
    _readers: dict[str, Type[IFileReader]] = {
        'csv': CSVReader,
        'xlsx': ExcelReader,
        'xls': ExcelReader,  # Soporte para archivos .xls
    }

    @classmethod
    def register_reader(cls, ext: str, reader_cls):
        cls._readers[ext.lower()] = reader_cls

    @classmethod
    def get_reader(cls, configFile: BaseFileConfig) -> IFileReader:
        ext = os.path.splitext(configFile.folder)[1][1:].lower()
        reader_cls = cls._readers.get(ext)
        if not reader_cls:
            raise ValueError(f"Formato de archivo '{ext}' no soportado")
        return reader_cls(configFile)
    

