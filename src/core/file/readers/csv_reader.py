from ..interfaces.file_reader import FileReaderInterface
import dask.dataframe as dd

class CSVReader(FileReaderInterface):
    def read(self, filepath: str, usecols=None, nrows=None) -> dd.DataFrame:
        try:
            df = dd.read_csv(filepath, usecols=usecols, blocksize="64MB")  # ajusta blocksize según tus recursos

            if nrows:
                df = df.head(nrows, compute=True)  # Esto devuelve un pandas.DataFrame si usas head()

            return df
        except Exception as e:
            raise ValueError(f"Error reading CSV file: {e}")
