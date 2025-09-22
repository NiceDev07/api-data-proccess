from modules.data_processing.domain.interfaces.pipelines import IPipeline
import polars as pl
from ..schemas.preload_camp_schema import DataProcessingDTO
from ..services.rne_service import RneService
from modules.data_processing.domain.constants.cols import Cols

class ExclutionRne(IPipeline):
    def __init__(self,
        rne_service: RneService,
        cols: Cols = Cols()
    ):
        self.rne_service =  rne_service
        self.cols = cols
        

    async def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        blacklist_set = await self.rne_service.get_list_rne()
        # Paso 4: Filtrar blacklist
        df = df.filter(~pl.col(self.cols.number_concat).is_in(blacklist_set))

        return df