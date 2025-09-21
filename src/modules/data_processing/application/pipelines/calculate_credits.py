from modules.data_processing.domain.interfaces.pipelines import IPipeline
import polars as pl
from ..schemas.preload_camp_schema import DataProcessingDTO

class CaculateCredits(IPipeline):
    def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        df = df.with_columns(
            (pl.col("__pdu_total__") * pl.col("__cost__"))
            .cast(pl.Float64)
            .alias("__credits__")
        )
        return df