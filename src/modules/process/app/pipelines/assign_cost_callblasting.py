import polars as pl
from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.constants.cols import Cols
from modules.process.app.services.cost import CostService


class AssignCostCallBlasting(IPipeline):
    """Asigna cost, cost_operator, initial e incremental desde CostRepository para call blasting."""

    default_cost = 0.0

    def __init__(self, cost_service: CostService):
        self._cost_service = cost_service

    async def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        rows = await self._cost_service.get_costs_cb(
            ctx.rulesCountry.idCountry, ctx.tariffId, ctx.subService
        )

        df = df.with_columns(pl.col(Cols.number_concat).cast(pl.Utf8))

        cost_expr        = pl.lit(self.default_cost)
        operator_expr    = pl.lit("")
        initial_expr     = pl.lit(self.default_cost)
        incremental_expr = pl.lit(self.default_cost)

        for prefix, cost, operator, initial, incremental in reversed(rows):
            cond = pl.col(Cols.number_concat).str.starts_with(prefix)
            cost_expr        = pl.when(cond).then(pl.lit(cost)).otherwise(cost_expr)
            operator_expr    = pl.when(cond).then(pl.lit(operator)).otherwise(operator_expr)
            initial_expr     = pl.when(cond).then(pl.lit(initial)).otherwise(initial_expr)
            incremental_expr = pl.when(cond).then(pl.lit(incremental)).otherwise(incremental_expr)

        return df.with_columns(
            cost_expr.alias(Cols.cost),
            operator_expr.alias(Cols.cost_operator),
            initial_expr.alias(Cols.initial),
            incremental_expr.alias(Cols.incremental),
        )
