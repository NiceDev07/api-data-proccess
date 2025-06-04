from .interfaces.file_reader import ObjectReaderInterface
import pandas as pd
import dask.dataframe as dd

class JSONReader(ObjectReaderInterface):
    def read(self, data: object) -> list:
        try:
            pdf = pd.DataFrame(data)
            df = dd.from_pandas(pdf, npartitions=1)
            return df
        except Exception as e:
            raise ValueError(f"Error reading JSON data: {e}")