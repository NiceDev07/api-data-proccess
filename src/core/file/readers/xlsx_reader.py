import dask.dataframe as dd
from ..interfaces.file_reader import IFileReader

class ExcelReader(IFileReader):
    def read(self, path: str) -> dd.DataFrame:
        return dd.read_excel(path, engine='openpyxl')

