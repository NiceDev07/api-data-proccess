from modules.data_processing.domain.interfaces.pipelines import IPipeline
import polars as pl
from ..schemas.preload_camp_schema import DataProcessingDTO
from ..services.rne_service import RneService

class ExclutionRne(IPipeline):
    def __init__(self, rne_service: RneService):
        self.rne_service =  rne_service
        

    def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        blacklist_set = self.rne_service.get_list_rne()
        # Paso 4: Filtrar blacklist
        df = df.filter(~pl.col("__number_concat__").is_in(blacklist_set))

        return df