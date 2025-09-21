from modules.data_processing.domain.interfaces.pipelines import IPipeline
import polars as pl
from ..schemas.preload_camp_schema import DataProcessingDTO
from ..services.numeration_service import NumerationService
from modules.data_processing.domain.constants.cols import Cols
import numpy as np

class AssignOperator(IPipeline):
    default_operator = "N/A"

    def __init__(self,
        numeration_service: NumerationService,
        cols: Cols = Cols()

    ):
        self.numeration_service = numeration_service
        self.cols = cols

    async def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        phone_column = ctx.configFile.nameColumnDemographic
        starts, ends, operators = await self.numeration_service.get_ranges(ctx.rulesCountry.idCountry)
        df = df.with_columns(pl.col(phone_column).cast(pl.Int64))

        # Obtener los números como array NumPy
        numbers = df[phone_column].to_numpy()

        # Buscar índice del rango en que caen
        idxs = np.searchsorted(starts, numbers, side="right") - 1
        valid = (idxs >= 0) & (numbers <= ends[idxs])
        assigned_ops = np.where(valid, operators[idxs], self.default_operator)

        # Insertar en el DataFrame de Polars
        return df.with_columns(pl.Series(self.cols.number_operator, assigned_ops))