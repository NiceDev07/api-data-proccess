import polars as pl
from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.constants.cols import Cols


# Fija PDU=1 en todos los registros para servicios donde la facturación es por evento
# (no por unidades de protocolo), de modo que CalculateCredits aplique cost × 1 directamente.
class AssignUnits(IPipeline):
    async def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        return df.with_columns(pl.lit(1).cast(pl.Int32).alias(Cols.pdu))
