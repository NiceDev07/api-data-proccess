from modules.process.domain.enums.file import FileType
from modules.process.app.files.csv_reader import CsvReader
from modules.process.app.files.xlsx_reader import XlsxReader
from modules.process.domain.interfaces.file_reader import IFileReader
from modules.process.app.files.null_reader import NullReader
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

    def create(self, file: str) -> IFileReader:
        if file is None:
            return NullReader()

        ext = self._get_extension(file)
        cls = self._map.get(ext)
        if not cls:
            raise ValueError(f"Servicio no soportado: {ext}")
        return cls()