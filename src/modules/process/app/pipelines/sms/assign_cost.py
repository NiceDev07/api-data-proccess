import polars as pl
from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.constants.cols import Cols
from modules.process.app.services.cost import CostService
from modules.process.infrastructure.repositories.cost import ServiceKey
from logging_config import get_logger

logger = get_logger(__name__)


class AssignCost(IPipeline):
    default_cost = 0.0

    def __init__(self, cost_service: CostService, service: ServiceKey):
        self.cost_service = cost_service
        self._service = service

    async def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        prefix_costs = await self.cost_service.get_costs(
            ctx.rulesCountry.idCountry, ctx.tariffId, self._service
        )

        if not prefix_costs:
            # Sin tarifa configurada el batch completo sale con cost=0.0 sin rastro —
            # el warning es la única señal de que falta configurar la tarifa (el vacío
            # además se cachea 1h, ver CostService.TTL).
            logger.warning(
                "Sin tarifa de costo configurada para country_id=%s tariff_id=%s service=%s — "
                "asignando cost=%.1f a todo el batch",
                ctx.rulesCountry.idCountry, ctx.tariffId, self._service, self.default_cost,
            )
            return df.with_columns(
                pl.lit(self.default_cost).alias(Cols.cost),
                pl.lit("").alias(Cols.cost_operator),
            )

        phone_col = Cols.number_concat
        df = df.with_columns(pl.col(phone_col).cast(pl.Utf8))

        prefixes, costs, operators = zip(*prefix_costs)
        lookup = (
            pl.DataFrame({
                "_prefix": list(prefixes),
                "_cost":   list(costs),
                "_op":     list(operators),
            })
            .with_columns(pl.col("_prefix").str.len_chars().alias("_plen"))
            .sort("_plen", descending=True)
        )

        # Longitudes únicas de prefijo, de mayor a menor (primera coincidencia = más específica)
        unique_lens: list[int] = lookup["_plen"].unique(maintain_order=True).to_list()

        for plen in unique_lens:
            subset = (
                lookup.filter(pl.col("_plen") == plen)
                .drop("_plen")
                .rename({"_prefix": f"_p{plen}", "_cost": f"_c{plen}", "_op": f"_o{plen}"})
            )
            df = (
                df.with_columns(pl.col(phone_col).str.slice(0, plen).alias(f"_p{plen}"))
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
