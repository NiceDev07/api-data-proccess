# app/core/file_reading/file_reader.py
import os
from dask.dataframe import DataFrame
from core.file.readers.factory import FileReaderFactory

class FileReader:
    def load(self, path: str, context: dict) -> DataFrame:
        if not os.path.isfile(path):
            raise FileNotFoundError(f"Archivo no encontrado: {path}")
        
        reader = FileReaderFactory.get_reader(path, context)
        return reader
