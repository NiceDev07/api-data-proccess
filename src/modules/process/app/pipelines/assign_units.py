import polars as pl
from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.constants.cols import Cols


class AssignUnits(IPipeline):
    """Sets PDU = 1 for all records.

    Used by services where billing is per-event (e.g. Call Blasting),
    so that CalculateCredits can apply cost × 1 without a PDU calculation.
    """

    async def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        return df.with_columns(pl.lit(1).cast(pl.Int32).alias(Cols.pdu))
