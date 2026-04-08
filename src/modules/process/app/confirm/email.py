import asyncio
from pathlib import Path
from typing import Any

import polars as pl

from modules.process.domain.constants.cols import Cols
from modules.process.infrastructure.repositories.email_confirm import EmailConfirmRepository
from modules.process.infrastructure.storage.local import LocalStorage
from modules.process.app.confirm.base import BaseConfirmStrategy

_COL_MAP = {
    Cols.email:   "mail",
    Cols.message: "body",
    Cols.subject: "subject",
}

_STATUS_PENDING  = "P"
_STATUS_EXCLUDED = "X"


class EmailConfirmStrategy(BaseConfirmStrategy):
    _service_name = "email"

    def __init__(self, repo: EmailConfirmRepository, storage: LocalStorage):
        super().__init__(storage)
        self._repo = repo

    async def _do_confirm(self, path: Path, campaign_ids: list[int]) -> dict[str, Any]:
        df = await asyncio.to_thread(pl.read_parquet, path)

        if df.is_empty():
            return {"inserted": 0, "message": "No records to insert."}

        df = self._map_columns(df)
        df = self._add_computed_columns(df)

        await self._repo.assert_campaigns_exist(campaign_ids)

        total = 0
        for campaign_id in campaign_ids:
            total += await self._repo.bulk_insert(campaign_id, df)

        return {"inserted": total}

    def _map_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        df = df.rename({k: v for k, v in _COL_MAP.items() if k in df.columns})

        # id_client: optional in parquet → default ""
        if Cols.identifier in df.columns and df[Cols.identifier].drop_nulls().len() > 0:
            df = df.rename({Cols.identifier: "id_client"})
        else:
            if Cols.identifier in df.columns:
                df = df.drop(Cols.identifier)
            df = df.with_columns(pl.lit("").alias("id_client"))

        # name_client: not in parquet → default ""
        df = df.with_columns(pl.lit("").alias("name_client"))

        return df

    def _add_computed_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        return (
            df
            .with_row_index("_idx")
            .with_columns(
                pl.when(pl.col(Cols.is_ok))
                  .then(pl.lit(_STATUS_PENDING))
                  .otherwise(pl.lit(_STATUS_EXCLUDED))
                  .alias("status"),
                ((pl.col("_idx") % 100) + 1).cast(pl.Utf8).alias("services"),
            )
            .drop([Cols.is_ok, Cols.error_code, Cols.cost, Cols.credits, Cols.email_domain, "_idx"])
        )
