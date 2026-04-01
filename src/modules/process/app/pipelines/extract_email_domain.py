import polars as pl
from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.constants.cols import Cols


class ExtractEmailDomain(IPipeline):
    """Extrae el dominio del email (gmail.com, hotmail.com…) para agrupación en el resumen."""

    async def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        c = ctx.configFile.nameColumnDemographic
        domain_expr = pl.col(c).str.splitn("@", 2).struct.field("field_1")
        return df.with_columns(domain_expr.alias(Cols.email_domain))
