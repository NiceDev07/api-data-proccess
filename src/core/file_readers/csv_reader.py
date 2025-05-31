from .interfaces.file_reader import FileReaderInterface

class CSVReader(FileReaderInterface):
    def read(self, filepath: str, usecols=None, nrows=None) -> list:
        try:
            print(f"Reading CSV file from: {filepath}")
            # df = pd.read_csv(filepath, usecols=usecols, nrows=nrows)
            # return df.to_dict(orient='records')
        except Exception as e:
            raise ValueError(f"Error reading CSV file: {e}")