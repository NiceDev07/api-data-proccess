from enum import Enum

class FileType(str, Enum):
    csv = "csv" # archivo csv
    json = "json" # archivo json
    parquet = "parquet" # archivo parquet Usado para big data
    xlsx = "xlsx" # excel
    gjson = "gjson" # es un json grande comprimido gzip
    json_embeddings = "json_embeddings" # significa que viene en el body de la request