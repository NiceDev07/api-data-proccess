import dask.dataframe as dd
from modules.data_processing.application.schemas.preload_camp_schema import DataProcessingDTO
from modules.data_processing.domain.policies.composition import CompositeCountryValidator
from modules.data_processing.domain.policies.validate_policies import (CharacterSpecialPolicy, CharacterLimitPolicy)
from modules.data_processing.application.services.forbbiden_works import ForbiddenWordsService
from modules.data_processing.application.factories.rules_country import RulesCountryFactory
from modules.data_processing.application.services.dataframe_preprocessor import DataFramePreprocessor
from modules.data_processing.domain.interfaces.black_list_crc_repository import IBlackListCRCRepository
from modules._common.domain.interfaces.file_reader import IFileReader
from modules.data_processing.domain.interfaces.numeracion_repository import INumeracionRepository
from modules.data_processing.application.services.operator_detector import OperatorDetector
from modules.data_processing.domain.interfaces.tariff_repository import ICostRepository
from modules.data_processing.application.services.cost_service import CostCalculatorService

class SMSUseCase:
    def __init__(
        self,
        df_processor: DataFramePreprocessor,
        forbidden_service: ForbiddenWordsService,
        blacklist_crc_repo: IBlackListCRCRepository,
        numeracion_repo: INumeracionRepository,
        tariff_repo: ICostRepository,
        exclusion_reader: IFileReader | None = None
    ):
        self.df_processor = df_processor
        self.forbidden_service = forbidden_service
        self.blacklist_crc_repo = blacklist_crc_repo
        self.exclusion_reader = exclusion_reader
        self.numeracion_repo = numeracion_repo
        self.tariff_repo = tariff_repo

    def execute(self, payload: DataProcessingDTO):
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
        
        # Cruce con la lista de exclusion general CRC (si aplica)
        list = self.blacklist_crc_repo.get_black_list_crc()
        df_clean["__number_concat__"] = str(payload.rulesCountry.codeCountry) + df_clean[number_column].astype(str)
        df_clean = df_clean[~df_clean["__number_concat__"].isin(list)]

        # Cruce de datos con la lista de exclusion (del usuario si lo requiere)
        if payload.configListExclusion:
            #TODO: Definir por que columna se cruza la lista de exclusión Y determinar nameColumnDemographic ya que si no tiene encabezado no se puede usar nameColumnDemographic directamente en usecols
            exclusion_column = payload.configListExclusion.nameColumnDemographic

            # Leer archivo de exclusión con las columnas necesarias
            df_exclusion = self.exclusion_reader.read(
                payload.configListExclusion.folder,
                usecols=[exclusion_column]
            )

            # Limpiar nulos y convertir a string para evitar errores de comparación
            df_exclusion = df_exclusion.dropna(subset=[exclusion_column])
            exclusion_values = df_exclusion[exclusion_column].astype(str)

            # Filtrar los números que NO están en la lista de exclusión
            df_clean[number_column] = df_clean[number_column].astype(str)
            df_clean = df_clean[~df_clean[number_column].isin(exclusion_values)]

        # Personalizacion de mensaje
        df_clean = df_clean.map_partitions(lambda pdf: pdf.assign(
            __message__=pdf.apply(lambda row: payload.content.format(**row), axis=1)
        ))

        # Cruce para identificar el operador del numero 
        ranges = self.numeracion_repo.get_numeracion(payload.rulesCountry.idCountry)
        dector = OperatorDetector(ranges=ranges)
        df_clean = dector.assign_operator(df_clean, number_column)
        result = self.tariff_repo.get_tariff_cost_data(payload.rulesCountry.idCountry, 1, "sms")
        
        # Calculo de costo del mensaje
        # Validar Rules del pais para determinar si todo esta OK con los mensajes personalizados

        # Crear un archivo parquet listo para insercion masiva en la base de datos
        # FIN: PROCESO BASE:
        
        # ... proccessos por subservicio (cada uno tiene un proceso diferente)
        # Proccesso Landing 
        # - Valida que el contido tenga una URL
        # - Agrega una url acortada al mensaje
        # - Se debe calcular el costo del mensaje

        return { 'success': True }