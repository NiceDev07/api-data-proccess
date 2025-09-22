from modules.data_processing.domain.interfaces.pipelines import IPipeline
import polars as pl
from ..schemas.preload_camp_schema import DataProcessingDTO
from modules.data_processing.domain.constants.cols import Cols

class ConcatPrefix(IPipeline):
    def __init__(
        self,
        cols: Cols = Cols()
    ):
        self.cols = cols

    def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        number_column = ctx.configFile.nameColumnDemographic
        prefix = str(ctx.rulesCountry.codeCountry)
        df = df.with_columns(
            (pl.lit(prefix) + pl.col(number_column)).alias(self.cols.number_concat)
        )
        return df