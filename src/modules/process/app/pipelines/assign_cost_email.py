import polars as pl
from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.constants.cols import Cols
from modules.process.app.services.cost import CostService


class AssignCostEmail(IPipeline):
    """Asigna cost y cost_operator para email según el dominio del correo.

    Requiere que ExtractEmailDomain haya poblado Cols.email_domain antes de este paso.
    Un prefijo vacío en la tabla de costos actúa como catch-all.
    """

    default_cost = 0.0

    def __init__(self, cost_service: CostService):
        self._cost_service = cost_service

    async def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        rows = await self._cost_service.get_costs(
            ctx.rulesCountry.idCountry, ctx.tariffId, "email"
        )

        if not rows:
            return df.with_columns(
                pl.lit(self.default_cost).alias(Cols.cost),
                pl.lit("").alias(Cols.cost_operator),
            )

        prefixes, costs, operators = zip(*rows)
        lookup = (
            pl.DataFrame({
                "_prefix": list(prefixes),
                "_cost":   list(costs),
                "_op":     list(operators),
            })
            .with_columns(pl.col("_prefix").str.len_chars().alias("_plen"))
            .sort("_plen", descending=True)
        )

        unique_lens: list[int] = lookup["_plen"].unique(maintain_order=True).to_list()
        domain_col = Cols.email_domain

        for plen in unique_lens:
            subset = (
                lookup.filter(pl.col("_plen") == plen)
                .drop("_plen")
                .rename({"_prefix": f"_p{plen}", "_cost": f"_c{plen}", "_op": f"_o{plen}"})
            )
            df = (
                df.with_columns(pl.col(domain_col).str.slice(0, plen).alias(f"_p{plen}"))
                .join(subset, on=f"_p{plen}", how="left")
                .drop(f"_p{plen}")
            )

        cost_cols = [f"_c{plen}" for plen in unique_lens]
        op_cols   = [f"_o{plen}" for plen in unique_lens]

        return (
            df
            .with_columns(
                pl.coalesce([pl.col(c) for c in cost_cols] + [pl.lit(self.default_cost)])
                .alias(Cols.cost),
                pl.coalesce([pl.col(c) for c in op_cols] + [pl.lit("")])
                .alias(Cols.cost_operator),
            )
            .drop(cost_cols + op_cols)
        )
