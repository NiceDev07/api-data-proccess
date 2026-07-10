import polars as pl
from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.domain.constants.cols import Cols
from modules.process.domain.models.process_dto import DataProcessingDTO


class ConcatPrefix(IPipeline):
    async def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        c = ctx.configFile.nameColumnDemographic
        prefix = int(ctx.rulesCountry.codeCountry)
        digits = ctx.rulesCountry.national_digits
        # factor desplaza el prefijo a la izquierda para concatenarlo aritméticamente
        factor = 10 ** digits

        return df.with_columns(
            (
                pl.lit(prefix, dtype=pl.Int64) * pl.lit(factor, dtype=pl.Int64)
                + pl.col(c).cast(pl.Int64, strict=False)
            ).alias(Cols.number_concat)
        )