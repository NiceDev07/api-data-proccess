from .interfaces.file_reader import ObjectReaderInterface

class JSONReader(ObjectReaderInterface):
    def read(self, data: object) -> list:
        try:
            print("Reading JSON data")
            # Assuming 'data' is a JSON string or a dictionary
            if isinstance(data, str):
                import json
                return json.loads(data)
            elif isinstance(data, dict):
                return [data]
            else:
                raise ValueError("Invalid data format for JSONReader")
        except Exception as e:
            raise ValueError(f"Error reading JSON data: {e}")