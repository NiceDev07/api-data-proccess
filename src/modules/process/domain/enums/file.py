from enum import Enum


class FileType(str, Enum):
    csv = "csv"
    json = "json"
    parquet = "parquet"
    xlsx = "xlsx"
    gjson = "gjson"               # JSON comprimido con gzip
    json_embeddings = "json_embeddings"  # payload JSON enviado directamente en el body