from modules.process.domain.enums.file import FileType
from modules.process.app.files.csv_reader import CsvReader
from modules.process.app.files.xlsx_reader import XlsxReader
from modules.process.domain.interfaces.file_reader import IFileReader
from modules.process.app.files.null_reader import NullReader
from logging_config import get_logger
import os

logger = get_logger(__name__)


class ReaderFileFactory:
    def __init__(self):
        self._map = {
            FileType.csv: CsvReader(),
            FileType.xlsx: XlsxReader(),
        }

    def _get_extension(self, file_name: str) -> str:
        _, ext = os.path.splitext(file_name.lower())
        return ext.replace(".", "")

    def create(self, file: str) -> IFileReader:
        if file is None:
            return NullReader()

        ext = self._get_extension(file)
        cls = self._map.get(ext)
        if not cls:
            logger.warning("Archivo sin extensión reconocida | path=%s | ext='%s'", file, ext or "vacía")
            raise FileNotFoundError("FILE_NOT_FOUND: Campaign file not found.")
        return cls