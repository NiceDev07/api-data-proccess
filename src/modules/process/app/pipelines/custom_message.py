import re
import polars as pl
from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.constants.cols import Cols

_TAG_RE = re.compile(r"\{(\w+(?:-\d+)?)\}")


class CustomMessage(IPipeline):
    async def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        ordered_tags = _TAG_RE.findall(ctx.content)
        if not ordered_tags:
            return df.with_columns(pl.lit(ctx.content).alias(Cols.message))

        template = _TAG_RE.sub("{}", ctx.content)
        
        return df.with_columns(
            pl.format(template, *[pl.col(tag).cast(pl.Utf8) for tag in ordered_tags])
            .alias(Cols.message)
        )
