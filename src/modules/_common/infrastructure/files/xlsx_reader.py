import dask.dataframe as dd
from modules._common.infrastructure.files.base_reader import BaseFileReader

class ExcelReader(BaseFileReader):
    def read(self, path: str) -> dd.DataFrame:
        self._check_file_exists(path)
        return dd.read_excel(path, engine='openpyxl')

