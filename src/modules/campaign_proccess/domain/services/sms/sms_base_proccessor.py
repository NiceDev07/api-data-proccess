import dask.dataframe as dd
from src.modules.campaign_proccess.domain.interfaces.proccess import ProcessorInterface

class SMSInformativoProcessor(ProcessorInterface):
    def __init__(self, regulation: dict):
        self.regulation = regulation

    def process(self, df: dd.DataFrame) -> dd.DataFrame:
        # Aquí la lógica de validación y filtrado con regulation
        return df
    
    #Common (CALL BLASTING, SMS)
    def operator_assignment(df) -> dd.DataFrame:
        # Aquí la lógica de asignación de operadores
        return df
    
    #Common (CALL BLASTING, SMS)
    def prefix_number(df: dd.DataFrame) -> dd.DataFrame:
        # Aquí la lógica de agregar prefijo a los números
        return df
    
    #Common ALL
    def customize_message(df: dd.DataFrame) -> dd.DataFrame:
        # Aquí la lógica de personalización del mensaje
        return df
    
    #Common ALL
    def black_list_check(df: dd.DataFrame) -> dd.DataFrame:
        # Aquí la lógica de verificación de listas negras
        return df
