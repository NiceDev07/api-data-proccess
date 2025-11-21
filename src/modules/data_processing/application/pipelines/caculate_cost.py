from modules.data_processing.domain.interfaces.pipelines import IPipeline
import polars as pl
from ..schemas.preload_camp_schema import DataProcessingDTO
from modules.data_processing.domain.constants.cols import Cols

class CalculateCost(IPipeline):
    def __init__(
        self,
        cols:Cols=Cols()
    ):
        self.cols = cols

    def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        
        df = df.with_columns(
            (pl.col(self.cols.pdu) * pl.col(self.cols.cost))
            .cast(pl.Float64)
            .alias(self.cols.credits)
        )
        return df