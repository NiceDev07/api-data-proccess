import asyncio
from pathlib import Path
from typing import Any

import polars as pl

from modules.process.domain.constants.cols import Cols
from modules.process.infrastructure.repositories.confirm.callblasting import CallBlastingConfirmRepository
from modules.process.infrastructure.storage.local import LocalStorage
from modules.process.app.confirm.base import BaseConfirmStrategy

_COL_MAP = {
    Cols.number_concat:   "phone",
    Cols.number_operator: "operator",
    Cols.message:         "text",
    Cols.credits:         "credits",
}

_DROP_COLS = [Cols.is_ok, Cols.error_code, Cols.seconds]


class CallBlastingConfirmStrategy(BaseConfirmStrategy):
    _service_name = "call_blasting"

    def __init__(self, repo: CallBlastingConfirmRepository, storage: LocalStorage):
        super().__init__(storage)
        self._repo = repo

    async def _do_confirm(self, path: Path, campaign_ids: list[int]) -> dict[str, Any]:
        lf = pl.scan_parquet(path).filter(pl.col(Cols.is_ok))
        lf = self._map_columns(lf)
        df = lf.collect()

        if df.is_empty():
            return {"inserted": 0, "message": "No valid records to insert."}

        await self._repo.create_campaign_tables(campaign_ids)

        results = await asyncio.gather(*[
            self._repo.bulk_insert(campaign_id, df)
            for campaign_id in campaign_ids
        ])

        return {"inserted": sum(results)}

    def _map_columns(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        schema = lf.collect_schema()

        rename_map = {k: v for k, v in _COL_MAP.items() if k in schema}
        if rename_map:
            lf = lf.rename(rename_map)

        if Cols.identifier in schema:
            lf = lf.with_columns(
                pl.col(Cols.identifier).fill_null("").alias("identification")
            ).drop(Cols.identifier)
        else:
            lf = lf.with_columns(pl.lit("").alias("identification"))

        cols_to_drop = [c for c in _DROP_COLS if c in schema]
        if cols_to_drop:
            lf = lf.drop(cols_to_drop)

        return lf
