from typing import Protocol
import polars as pl

class IExclusionSource(Protocol):
    async def get_df(self, ctx) -> pl.DataFrame:
        ...