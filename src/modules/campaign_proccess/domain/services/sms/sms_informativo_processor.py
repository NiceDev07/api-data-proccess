import dask.dataframe as dd
from src.modules.campaign_proccess.domain.interfaces.proccess import ProcessorInterface

class SMSInformativoProcessor(ProcessorInterface):
    def __init__(self, regulation: dict):
        self.regulation = regulation

    def process(self, df: dd.DataFrame) -> dd.DataFrame:
        # Aquí la lógica de validación y filtrado con regulation
        return df
