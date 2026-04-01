import polars as pl
from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.constants.cols import Cols
from modules.process.app.services.cost import CostService
from modules.process.infrastructure.repositories.cost import ServiceKey


class AssignCost(IPipeline):
    default_cost = 0.0

    def __init__(self, cost_service: CostService, service: ServiceKey | None = None):
        self.cost_service = cost_service
        self._service = service  # None → use ctx.subService at runtime

    async def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        phone_column = Cols.number_concat
        service = self._service if self._service is not None else ctx.subService
        prefix_costs = await self.cost_service.get_costs(
            ctx.rulesCountry.idCountry, ctx.tariffId, service
        )

        df = df.with_columns(pl.col(phone_column).cast(pl.Utf8))

        cost_expr = pl.lit(self.default_cost)
        cost_operator_expr = pl.lit("")

        # Iterar de más corto a más largo: el prefijo más largo queda en la capa
        # exterior del when/then/otherwise y tiene prioridad en la evaluación.
        for prefix, cost, cost_operator in reversed(prefix_costs):
            condition = pl.col(phone_column).str.starts_with(prefix)
            cost_expr = pl.when(condition).then(pl.lit(cost)).otherwise(cost_expr)
            cost_operator_expr = (
                pl.when(condition).then(pl.lit(cost_operator)).otherwise(cost_operator_expr)
            )

        return df.with_columns(
            cost_expr.alias(Cols.cost),
            cost_operator_expr.alias(Cols.cost_operator),
        )
