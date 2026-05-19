import polars as pl

from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.constants.cols import Cols
from modules.process.app.pipelines._template import build_template_expr


class CustomSubject(IPipeline):
    async def execute(self, df: pl.DataFrame | pl.LazyFrame, ctx: DataProcessingDTO) -> pl.DataFrame | pl.LazyFrame:
        if not ctx.subject:
            raise ValueError("SUBJECT_REQUIRED: The 'subject' field is required for the email service.")
        cols = df.collect_schema().names() if isinstance(df, pl.LazyFrame) else df.columns
        return df.with_columns(build_template_expr(ctx.subject, Cols.subject, cols))
