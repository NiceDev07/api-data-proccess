import polars as pl
from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.constants.cols import Cols
from modules.process.app.services.cost import CostService


class AssignCostCallBlasting(IPipeline):
    """Asigna cost, cost_operator, initial e incremental para call blasting."""

    default_cost = 0.0

    def __init__(self, cost_service: CostService):
        self._cost_service = cost_service

    async def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        rows = await self._cost_service.get_costs_cb(
            ctx.rulesCountry.idCountry, ctx.tariffId, ctx.subService
        )

        if not rows:
            return df.with_columns(
                pl.lit(self.default_cost).alias(Cols.cost),
                pl.lit("").alias(Cols.cost_operator),
                pl.lit(self.default_cost).alias(Cols.initial),
                pl.lit(self.default_cost).alias(Cols.incremental),
            )

        df = df.with_columns(pl.col(Cols.number_concat).cast(pl.Utf8))

        prefixes, costs, operators, initials, incrementals = zip(*rows)
        lookup = (
            pl.DataFrame({
                "_prefix":      list(prefixes),
                "_cost":        list(costs),
                "_op":          list(operators),
                "_initial":     list(initials),
                "_incremental": list(incrementals),
            })
            .with_columns(pl.col("_prefix").str.len_chars().alias("_plen"))
            .sort("_plen", descending=True)
        )

        unique_lens: list[int] = lookup["_plen"].unique(maintain_order=True).to_list()
        phone_col = Cols.number_concat

        for plen in unique_lens:
            subset = (
                lookup.filter(pl.col("_plen") == plen)
                .drop("_plen")
                .rename({
                    "_prefix":      f"_p{plen}",
                    "_cost":        f"_c{plen}",
                    "_op":          f"_o{plen}",
                    "_initial":     f"_i{plen}",
                    "_incremental": f"_inc{plen}",
                })
            )
            df = (
                df.with_columns(pl.col(phone_col).str.slice(0, plen).alias(f"_p{plen}"))
                .join(subset, on=f"_p{plen}", how="left")
                .drop(f"_p{plen}")
            )

        cost_cols = [f"_c{plen}"   for plen in unique_lens]
        op_cols   = [f"_o{plen}"   for plen in unique_lens]
        ini_cols  = [f"_i{plen}"   for plen in unique_lens]
        inc_cols  = [f"_inc{plen}" for plen in unique_lens]

        return (
            df
            .with_columns(
                pl.coalesce([pl.col(c) for c in cost_cols] + [pl.lit(self.default_cost)])
                .alias(Cols.cost),
                pl.coalesce([pl.col(c) for c in op_cols] + [pl.lit("")])
                .alias(Cols.cost_operator),
                pl.coalesce([pl.col(c) for c in ini_cols] + [pl.lit(self.default_cost)])
                .alias(Cols.initial),
                pl.coalesce([pl.col(c) for c in inc_cols] + [pl.lit(self.default_cost)])
                .alias(Cols.incremental),
            )
            .drop(cost_cols + op_cols + ini_cols + inc_cols)
        )
