import polars as pl
from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.domain.interfaces.storage import IStorage
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.constants.cols import Cols

class SaveResults(IPipeline):
    _AUDIT_COLS = [Cols.is_ok, Cols.error_code]

    def __init__(self, cols: list[str], storage: IStorage, service: str):
        self.cols = cols
        self.storage = storage
        self.service = service

    async def execute(self, df: pl.DataFrame | pl.LazyFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        if isinstance(df, pl.LazyFrame):
            df = df.collect(engine="streaming")
        key = ctx.codeGroup if ctx.codeGroup else "-".join(str(c) for c in ctx.campaignId)
        filename = f"Campaign/{self.service}/campaign_{key}.parquet"
        save_cols = self.cols + self._AUDIT_COLS

        # Si el identificador está en la lista de columnas esperadas, lo excluimos
        # del parquet cuando: (a) la columna no llegó al DataFrame (SMS/CallBlasting
        # que aún no implementan el feature), o (b) llegó pero está completamente vacía.
        if Cols.identifier in save_cols:
            if Cols.identifier not in df.columns or (
                df[Cols.identifier].cast(pl.Utf8).str.strip_chars().eq("").all()
            ):
                save_cols = [c for c in save_cols if c != Cols.identifier]

        await self.storage.save(df.select(save_cols), filename)
        return df
