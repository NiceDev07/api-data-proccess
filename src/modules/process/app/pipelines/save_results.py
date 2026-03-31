from uuid import uuid4
import polars as pl
from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.domain.interfaces.storage import IStorage
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.constants.cols import Cols


class SaveResults(IPipeline):
    _AUDIT_COLS = [Cols.is_ok, Cols.error_code]

    def __init__(self, cols: list[str], storage: IStorage):
        self.cols = cols
        self.storage = storage

    async def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        filename = f"{ctx.campaignId[0]}_{ctx.subService}_{uuid4().hex[:8]}.parquet"
        await self.storage.save(df.select(self.cols + self._AUDIT_COLS), filename)
        return df
