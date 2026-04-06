import polars as pl
from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.constants.cols import Cols
from modules.process.app.services.cost import CostService


class AssignCostEmail(IPipeline):
    default_cost = 0.0

    def __init__(self, cost_service: CostService):
        self._cost_service = cost_service

    async def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        cost = await self._cost_service.get_email_cost(
            ctx.rulesCountry.idCountry, ctx.tariffId
        )

        return df.with_columns(
            pl.lit(cost if cost is not None else self.default_cost).alias(Cols.cost),
        )
