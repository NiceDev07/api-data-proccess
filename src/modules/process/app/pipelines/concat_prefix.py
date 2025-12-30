from modules.process.domain.interfaces.pipeline import IPipeline
import polars as pl
from modules.process.domain.constants.cols import Cols
from modules.process.domain.models.process_dto import DataProcessingDTO

class ConcatPrefix(IPipeline):
    async def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        c = ctx.configFile.nameColumnDemographic

        # prefijo como int
        prefix_int = int(ctx.rulesCountry.codeCountry)

        # cantidad de dígitos del número nacional (elige el mayor por seguridad)
        digits = max(ctx.rulesCountry.numberDigitsFixed, ctx.rulesCountry.numberDigitsMobile)

        # 10^digits (constante) -> súper rápido
        factor = 10 ** digits

        return df.with_columns(
            (
                pl.lit(prefix_int, dtype=pl.Int64) * pl.lit(factor, dtype=pl.Int64)
                + pl.col(c).cast(pl.Int64, strict=False)
            ).alias(Cols.number_concat)
        )