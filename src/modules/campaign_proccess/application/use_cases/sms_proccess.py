import dask.dataframe as dd
from modules.campaign_proccess.application.schemas.preload_camp_schema import PreloadCampDTO
from modules.campaign_proccess.domain.policies.composition import CompositeCountryValidator
from modules.campaign_proccess.domain.policies.validate_policies import (CharacterSpecialPolicy, CharacterLimitPolicy)
from modules.campaign_proccess.application.services.forbbiden_works import ForbiddenWordsService
from modules.campaign_proccess.application.factories.rules_country import RulesCountryFactory
from modules.campaign_proccess.application.services.dataframe_preprocessor import DataFramePreprocessor
from modules.campaign_proccess.domain.interfaces.black_list_crc_repository import IBlackListCRCRepository

class SMSUseCase:
    def __init__(
        self,
        df_processor: DataFramePreprocessor,
        forbidden_service: ForbiddenWordsService,
        blacklist_crc_repo: IBlackListCRCRepository
    ):
        self.df_processor = df_processor
        self.forbidden_service = forbidden_service
        self.blacklist_crc_repo = blacklist_crc_repo

    def execute(self, payload: PreloadCampDTO):
        rules_country = RulesCountryFactory.from_dto(payload.rulesCountry)
        # Se hace una primera validacion del mensaje base para determinar si cumple con las reglas del pais
        # Asi si no cumple, no se procede a leer el archivo evitando proccesos innecesarios (luego cuando se proccessa de nuevo se valida de nuevo) 
        quick_validators = CompositeCountryValidator(validators=[CharacterSpecialPolicy(), CharacterLimitPolicy()])
        quick_validators.ensure_valid(
            content=payload.content,
            rules=rules_country
        )
        self.forbidden_service.ensure_message_is_valid(payload.content, 4757)
        df = self.df_processor.load_dataframe(payload)
        number_column = payload.configFile.nameColumnDemographic
        
        # START: PROCESO BASE (Logica compartida):
        # Limpiar el DataFrame eliminando todos los datos que esten vacios en la columna de numero de telefono (guardar este dato, cuantos se eliminaron)
        df[number_column] = dd.to_numeric(df[number_column], errors='coerce')
        df_clean = df.dropna(subset=[number_column])
        list = self.blacklist_crc_repo.get_black_list_crc()
        df_clean["__number_concat__"] = str(payload.rulesCountry.codeCountry) + df_clean[number_column].astype(str)
        df_clean = df_clean[~df_clean["__number_concat__"].isin(list)]
        
        # class cleaned_df:
        # Cruce con la lista de exclusion general CRC (si aplica)
        # Cruce de datos con la lista de exclusion (del usuario si lo requiere)
        # Cruce para identificar el operador del numero 

        # Personalizacion de mensaje 
        # Calculo de costo del mensaje
        # Crear un archivo parquet listo para insercion masiva en la base de datos
        # FIN: PROCESO BASE:
        
        # ... proccessos por subservicio (cada uno tiene un proceso diferente)
        # Proccesso Landing 
        # - Valida que el contido tenga una URL
        # - Agrega una url acortada al mensaje
        # - Se debe calcular el costo del mensaje

        return { 'success': True }