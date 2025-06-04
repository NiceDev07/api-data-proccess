import dask.dataframe as dd
from modules.campaign_proccess.domain.interfaces.proccess import ProcessorInterface

class SMSInformativoProcessor(ProcessorInterface):
    def __init__(self, regulation: dict):
        self.regulation = regulation

    def process(self, df: dd.DataFrame) -> dd.DataFrame:
        # Aquí la lógica de validación y filtrado con regulation
        df = self.operator_assignment(df)
        df = self.prefix_number(df)
        df = self.customize_message(df)
        df = self.black_list_check(df)

        return df
    
    #Common (CALL BLASTING, SMS)
    def operator_assignment(self, df) -> dd.DataFrame:
        df['OPERATOR'] = 'CLARO'
        # Aquí la lógica de asignación de operadores
        return df
    
    #Common (CALL BLASTING, SMS)
    def prefix_number(self, df: dd.DataFrame) -> dd.DataFrame:
        df['PHONE'] = 0
        # Aquí la lógica de agregar prefijo a los números
        return df
    
    #Common ALL
    def customize_message(self, df: dd.DataFrame) -> dd.DataFrame:
        # Aquí la lógica de personalización del mensaje
        df['MESSAGE'] = 'Mensaje personalizado'
        return df
    
    #Common ALL
    def black_list_check(self, df: dd.DataFrame) -> dd.DataFrame:
        # Aquí la lógica de verificación de listas negras
        df['BLACK_LIST'] = False
        return df
