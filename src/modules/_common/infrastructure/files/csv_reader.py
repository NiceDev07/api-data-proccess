from modules._common.infrastructure.files.base_reader import BaseFileReader
import polars as pl
from modules.data_processing.application.schemas.preload_camp_schema import BaseFileConfig 

class CSVReader(BaseFileReader):
    def __init__(self, configFile: BaseFileConfig):
        self.sep = configFile.delimiter
        self.header = configFile.useHeaders

    def read(self, filepath: str, usecols=None, nrows=None) -> pl.DataFrame:
        try:
            self._check_file_exists(filepath)

            df = pl.read_csv(
                filepath,
                separator=self.sep,
                has_header=self.header,
                columns=usecols,
                n_rows=nrows,
                try_parse_dates=True,
                encoding="utf8-lossy",  # Evita errores de decodificación
            )

            return df

        except pl.exceptions.ComputeError as e:
            raise ValueError(f"Error reading CSV file with Polars: {e}")
        except FileNotFoundError:
            raise FileNotFoundError(f"File not found: {filepath}")
        except Exception as e:
            raise RuntimeError(f"Unexpected error while reading CSV file: {str(e)}")


# Register the readers
