from modules.data_processing.domain.interfaces.pipelines import IPipeline
import polars as pl
from ..schemas.preload_camp_schema import DataProcessingDTO
from modules.data_processing.domain.constants.cols import Cols

class CustomMessage(IPipeline):
    def __init__(self,
        template_polars: str,
        ordered_tags: list[str],
        cols: Cols = Cols()
    ):
        self.template_polars = template_polars
        self.ordered_tags = ordered_tags
        self.cols = cols

    def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        if len(self.ordered_tags) == 0:
            return df
        
        df = df.with_columns(
            pl.format(
                self.template_polars,
                *[pl.col(tag).cast(pl.Utf8) for tag in self.ordered_tags]
            ).alias(self.cols.message)
        )
        return df