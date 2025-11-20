from modules.process.domain.enums.file import FileType
from modules.process.infrastructure.files.csv_reader import CsvReader
from modules.process.infrastructure.files.xlsx_reader import XlsxReader
from modules.process.domain.models.process_dto import BaseFileConfig
import os

class ReaderFileFactory:
    def __init__(self):
        self._map = {
            FileType.csv: CsvReader,
            FileType.xlsx: XlsxReader,  # Placeholder for XLSX reader
        }
    
    def _get_extension(self, file_name: str) -> str:
        _, ext = os.path.splitext(file_name.lower())
        return ext.replace(".", "")  # ".csv" -> "csv"

    def create(self, file: BaseFileConfig):
        ext = self._get_extension(file.folder)
        print(f"Creating file reader for extension: {ext}")
        cls = self._map.get(ext)
        if not cls:
            raise ValueError(f"Servicio no soportado: {file.folder}")
        return cls()