from modules.data_processing.domain.interfaces.pipelines import IPipeline
import polars as pl
from ..schemas.preload_camp_schema import DataProcessingDTO
from modules.data_processing.application.services.cost_service import CostService
from modules.data_processing.domain.constants.cols import Cols

class AssignCost(IPipeline):
    default_cost = 0.0

    def __init__(
        self,
        cost_service: CostService,
        cols: Cols = Cols()
    ):
        self.cost_service = cost_service
        self.cols = cols


    async def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        phone_column = self.cols.number_concat
        prefix_costs = await self.cost_service.get_costs(ctx.rulesCountry.idCountry, ctx.tariffId, "sms")

        df = df.with_columns(pl.col(phone_column).cast(pl.Utf8))
        # Inicializar expresión de costo
        costo_expr = pl.lit(self.default_cost)

        # Construir la expresión condicional
        for prefix, cost, cost_operator in prefix_costs:
            costo_expr = (
                pl.when(pl.col(phone_column).str.starts_with(prefix))
                .then(pl.lit(cost))
                .otherwise(costo_expr)
            )

        return df.with_columns(costo_expr.alias("__cost__"), pl.lit(cost_operator).alias("__cost_operator__"))