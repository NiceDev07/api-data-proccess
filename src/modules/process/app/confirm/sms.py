import asyncio
from pathlib import Path
from typing import Any

import polars as pl

from modules.process.domain.constants.cols import Cols
from modules.process.infrastructure.repositories.confirm.sms import SmsConfirmRepository
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

_DROP_COLS = [Cols.is_ok, Cols.error_code]


class SmsConfirmStrategy(BaseConfirmStrategy):
    _service_name = "sms"

    def __init__(self, repo: SmsConfirmRepository, storage: LocalStorage):
        super().__init__(storage)
        self._repo = repo

    async def _do_confirm(self, path: Path, _campaign_ids: list[int]) -> dict[str, Any]:
        lf = pl.scan_parquet(path)
        lf = self._map_columns(lf)
        lf = self._add_computed_columns(lf)
        df = lf.collect()

        if df.is_empty():
            return {"inserted": 0, "message": "No records to insert."}

        # Create all tables in one transaction before parallel inserts
        await self._repo.create_campaign_tables(_campaign_ids)

        results = await asyncio.gather(*[
            self._repo.bulk_insert(
                campaign_id,
                df.with_columns(pl.lit(campaign_id).alias("id_campana")),
            )
            for campaign_id in _campaign_ids
        ])

        return {"inserted": sum(results)}

    def _map_columns(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        schema = lf.collect_schema()

        rename_map = {k: v for k, v in _COL_MAP.items() if k in schema}
        if rename_map:
            lf = lf.rename(rename_map)

        if Cols.identifier in schema:
            lf = (
                lf
                .with_columns(pl.col(Cols.identifier).fill_null("").alias("identificacion"))
                .drop(Cols.identifier)
            )
        else:
            lf = lf.with_columns(pl.lit("").alias("identificacion"))

        return lf

    def _add_computed_columns(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        schema = lf.collect_schema()
        cols_to_drop = [c for c in _DROP_COLS if c in schema] + ["_idx"]
        return (
            lf
            .with_row_index("_idx")
            .with_columns(
                pl.when(pl.col(Cols.is_ok))
                  .then(pl.lit(_STATUS_PENDING))
                  .otherwise(pl.lit(_STATUS_EXCLUDED))
                  .alias("estado"),
                ((pl.col("_idx") % 100) + 1).cast(pl.Utf8).alias("servicio"),
            )
            .drop(cols_to_drop)
        )
