from dask import dataframe as dd
from core.file.interfaces.file_reader import IFileReader
from modules.campaign_proccess.application.schemas.preload_camp_schema import PreloadCampDTO
from modules.campaign_proccess.application.helpers.tags import extract_tags_from_content
from modules.campaign_proccess.application.helpers.required_columns import build_required_columns

class DataFramePreprocessor:
    def __init__(self, file_reader: IFileReader):
        self.file_reader = file_reader

    def load_dataframe(self, payload: PreloadCampDTO) -> dd.DataFrame:
        file_path = payload.configFile.folder
        content_tags = extract_tags_from_content(payload.content)
        usecols = build_required_columns(list(content_tags), payload.configFile)

        df = self.file_reader.read(file_path, usecols=usecols)

        if not payload.configFile.useHeaders:
            df = df.map_partitions(self._rename_columns)

        return df

    def _rename_columns(self, df_partition):
        df_partition.columns = [f"Col-{i+1}" for i in df_partition.columns]
        return df_partition
