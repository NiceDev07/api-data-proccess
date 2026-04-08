import asyncio
from pathlib import Path
from typing import Any

import polars as pl

from modules.process.domain.constants.cols import Cols
from modules.process.infrastructure.repositories.sms_confirm import SmsConfirmRepository
from modules.process.infrastructure.storage.local import LocalStorage
from modules.process.app.confirm.base import BaseConfirmStrategy

# Parquet columns → DB table columns
_COL_MAP = {
    Cols.number_concat:   "celular",
    Cols.message:         "texto",
    Cols.number_operator: "operador",
    Cols.pdu:             "pdu",
    Cols.credits:         "credit",
}

_STATUS_PENDING  = "P"
_STATUS_EXCLUDED = "X"


class SmsConfirmStrategy(BaseConfirmStrategy):
    _service_name = "sms"

    def __init__(self, repo: SmsConfirmRepository, storage: LocalStorage):
        super().__init__(storage)
        self._repo = repo

    async def _do_confirm(self, path: Path, _campaign_ids: list[int]) -> dict[str, Any]:
        df = await asyncio.to_thread(pl.read_parquet, path)

        if df.is_empty():
            return {"inserted": 0, "message": "No records to insert."}

        df = self._map_columns(df)
        df = self._add_computed_columns(df)

        # Table creation must be sequential (shared async session)
        for campaign_id in _campaign_ids:
            await self._repo.create_campaign_table(campaign_id)

        # Bulk inserts are independent — run concurrently across campaigns
        results = await asyncio.gather(*[
            self._repo.bulk_insert(
                campaign_id,
                df.with_columns(pl.lit(campaign_id).alias("id_campana")),
            )
            for campaign_id in _campaign_ids
        ])

        return {"inserted": sum(results)}

    def _map_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        df = df.rename({k: v for k, v in _COL_MAP.items() if k in df.columns})

        # identificacion: optional in parquet, required (NOT NULL) in table → default ""
        if Cols.identifier in df.columns and df[Cols.identifier].drop_nulls().len() > 0:
            df = df.rename({Cols.identifier: "identificacion"})
        else:
            if Cols.identifier in df.columns:
                df = df.drop(Cols.identifier)
            df = df.with_columns(pl.lit("").alias("identificacion"))

        return df

    def _add_computed_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        return (
            df
            .with_row_index("_idx")
            .with_columns(
                pl.when(pl.col(Cols.is_ok))
                  .then(pl.lit(_STATUS_PENDING))
                  .otherwise(pl.lit(_STATUS_EXCLUDED))
                  .alias("estado"),
                # servicio is VARCHAR(3) in the table
                ((pl.col("_idx") % 100) + 1).cast(pl.Utf8).alias("servicio"),
            )
            .drop([Cols.is_ok, Cols.error_code, "_idx"])
        )
