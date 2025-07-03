import polars as pl
from modules._common.domain.interfaces.file_reader import IFileReader
from modules.data_processing.application.schemas.preload_camp_schema import DataProcessingDTO
from modules.data_processing.application.helpers.tags import extract_tags_from_content
from modules.data_processing.application.helpers.required_columns import build_required_columns

class DataFramePreprocessor:
    def __init__(self, file_reader: IFileReader):
        self.file_reader = file_reader

    def load_dataframe(self, payload: DataProcessingDTO) -> pl.DataFrame:
        file_path = payload.configFile.folder
        content_tags = extract_tags_from_content(payload.content)
        usecols = build_required_columns(list(content_tags), payload.configFile)

        # 📥 Leer CSV como Polars
        df = self.file_reader.read(file_path, usecols=usecols)

        # ⚠️ Si no tiene headers, renombrar columnas
        if not payload.configFile.useHeaders:
            df = self._rename_columns(df)

        return df

    def _rename_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        new_columns = [f"Col-{i+1}" for i in range(len(df.columns))]
        return df.rename(dict(zip(df.columns, new_columns)))
