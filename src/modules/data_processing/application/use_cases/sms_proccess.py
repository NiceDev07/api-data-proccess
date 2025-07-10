import re
import polars as pl
from modules.data_processing.domain.services.cost_assigner import CostAssigner
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
from modules.data_processing.application.helpers.tags import extract_tags_with_repeats

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
        # Paso 1: Convertir a string y eliminar vacíos y nulos
        df = df.with_columns(
            pl.col(number_column).cast(pl.Utf8).alias(number_column)
        )
        df = df.filter(
            (pl.col(number_column).is_not_null()) &
            (pl.col(number_column).str.strip_chars().str.len_chars() > 0)
        )

        # Paso 2: Obtener blacklist como set
        blacklist_set = set(self.blacklist_crc_repo.get_black_list_crc())

        # Paso 3: Concatenar código de país
        prefix = str(payload.rulesCountry.codeCountry)
        df = df.with_columns(
            (pl.lit(prefix) + pl.col(number_column)).alias("__number_concat__")
        )

        # Paso 4: Filtrar blacklist
        df = df.filter(~pl.col("__number_concat__").is_in(blacklist_set))


        # Cruce de datos con la lista de exclusion (del usuario si lo requiere)
        if payload.configListExclusion:
            exclusion_column = payload.configListExclusion.nameColumnDemographic

            # Leer archivo de exclusión
            df_exclusion = self.exclusion_reader.read(
                payload.configListExclusion.folder,
                usecols=[exclusion_column]
            )

            # Limpiar nulos y convertir a string
            df_exclusion = df_exclusion.with_columns(
                pl.col(exclusion_column).cast(pl.Utf8)
            ).filter(
                pl.col(exclusion_column).is_not_null() &
                (pl.col(exclusion_column).str.strip_chars().str.len_chars() > 0)
            )

            # Obtener valores únicos de exclusión como set
            exclusion_values = set(df_exclusion[exclusion_column].to_list())

            # Asegurarse de que el número esté en string
            df = df.with_columns(
                pl.col(number_column).cast(pl.Utf8)
            )

            # Filtrar registros que NO estén en la lista de exclusión
            df = df.filter(~pl.col(number_column).is_in(exclusion_values))

        # # Personalizacion de mensaje
        ordered_tags = extract_tags_with_repeats(payload.content)
        template_polars = re.sub(r"{(\w+(?:-\d+)?)\}", "{}", payload.content)

        # Valida si las etiquetas usadas no contienen palabras prohibidas
        self.forbidden_service.ensure_dataframe_values_are_valid(
            df,
            ordered_tags,
            4757
        )

        #Personaliza el mensaje con las etiquetas
        df = df.with_columns(
            pl.format(
                template_polars,
                *[pl.col(tag).cast(pl.Utf8) for tag in ordered_tags]
            ).alias("__message__")
        )

        # # Cruce para identificar el operador del numero 
        ranges = self.numeracion_repo.get_numeracion(payload.rulesCountry.idCountry)
        dector = OperatorDetector(ranges=ranges)
        df = dector.assign_operator(df, number_column)
       
        # df.write_parquet("resultados/conoperator.parquet", compression="zstd")
        # Obtiene los costos de los prefijos
        result = self.tariff_repo.get_prefix_cost_pairs(payload.rulesCountry.idCountry, 1, "sms")
        cost_asigner = CostAssigner(result, default_cost=0.0)
        df = cost_asigner.assign_cost(df, "__number_concat__")
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