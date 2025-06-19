from modules._common.infrastructure.files.base_reader import BaseFileReader
import dask.dataframe as dd

class CSVReader(BaseFileReader):
    def __init__(self, sep: str = ";", header: bool = True):
        self.sep = sep
        self.header = header

    def read(self, filepath: str, usecols=None, nrows=None) -> dd.DataFrame:
        try:
            self._check_file_exists(filepath)
            df = dd.read_csv(filepath, sep=self.sep, dtype=str, assume_missing=True, usecols=usecols, blocksize="64MB", header=0 if self.header else None)  # ajusta blocksize según tus recursos
            return df
        except UnicodeDecodeError as e:
            raise ValueError(f"Error decoding CSV file: {e}")
        except ValueError as e:
            raise ValueError(f"Value error while reading CSV file: {str(e)}. Please check the CSV format and provided parameters.")
        except Exception as e:
            raise RuntimeError(f"Unexpected error while reading CSV file: {str(e)}")


# Register the readers
