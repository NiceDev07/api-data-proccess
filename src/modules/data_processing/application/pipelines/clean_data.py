from modules.data_processing.domain.interfaces.pipelines import IPipeline
import polars as pl
from ..schemas.preload_camp_schema import DataProcessingDTO

class CleanData(IPipeline):
    def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        number_column = ctx.configFile.nameColumnDemographic
        df = df.with_columns(
            pl.col(number_column).cast(pl.Utf8).alias(number_column)
        )
        df = df.filter(
            (pl.col(number_column).is_not_null()) &
            (pl.col(number_column).str.strip_chars().str.len_chars() > 0)
        )

        return df