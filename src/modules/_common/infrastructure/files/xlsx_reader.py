from modules._common.infrastructure.files.base_reader import BaseFileReader
import polars as pl

class ExcelReader(BaseFileReader):
    def read(self, path: str) -> pl.DataFrame :
        self._check_file_exists(path)
        return pl.read_excel(path, engine='openpyxl')

