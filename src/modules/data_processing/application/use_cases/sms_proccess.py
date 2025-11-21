import re
import polars as pl
from modules.data_processing.application.schemas.preload_camp_schema import DataProcessingDTO
from modules.data_processing.domain.policies.composition import CompositeCountryValidator
from modules.data_processing.domain.policies.validate_policies import (CharacterSpecialPolicy, CharacterLimitPolicy)
from modules.data_processing.application.services.forbbiden_works import ForbiddenWordsService
from modules.data_processing.application.factories.rules_country import RulesCountryFactory
from modules.data_processing.application.services.dataframe_preprocessor import DataFramePreprocessor
from modules._common.domain.interfaces.file_reader import IFileReader
from modules.data_processing.domain.interfaces.numeracion_repository import INumeracionRepository
from modules.data_processing.domain.interfaces.tariff_repository import ICostRepository
from modules.data_processing.application.helpers.tags import extract_tags_with_repeats
from modules.data_processing.application.pipelines.clean_data import CleanData
from modules.data_processing.application.pipelines.concat_prefix import ConcatPrefix
from modules.data_processing.application.pipelines.exclusion_rne import ExclutionRne
from modules.data_processing.application.pipelines.assign_operator import AssignOperator
from modules.data_processing.application.pipelines.custom_message import CustomMessage
from modules.data_processing.application.pipelines.validate_level import ValidateLevel
from modules.data_processing.application.pipelines.caculate_pdu import CalculatePDU
from modules.data_processing.application.pipelines.assign_cost import AssignCost
from modules.data_processing.application.pipelines.calculate_credits import CaculateCredits
from ..services.rne_service import RneService
from ..services.numeration_service import NumerationService
from modules.data_processing.domain.constants.cols import Cols
from modules.data_processing.application.services.cost_service import CostService

class SMSUseCase:
    def __init__(
        self,
        df_processor: DataFramePreprocessor,
        forbidden_service: ForbiddenWordsService,
        rne_service: RneService,
        numeracion_repo: INumeracionRepository,
        tariff_repo: ICostRepository,
        exclusion_reader: IFileReader | None = None,
        number_service: NumerationService = None,
        cost_service: CostService = None
    ):
        self.df_processor = df_processor
        self.forbidden_service = forbidden_service
        self.rne_service = rne_service
        self.exclusion_reader = exclusion_reader
        self.numeracion_repo = numeracion_repo
        self.tariff_repo = tariff_repo
        self.number_service = number_service
        self.cost_service = cost_service
        self.cols = Cols()

    async def execute(self, payload: DataProcessingDTO):
        rules_country = RulesCountryFactory.from_dto(payload.rulesCountry)
        # Se hace una primera validacion del mensaje base para determinar si cumple con las reglas del pais
        # Asi si no cumple, no se procede a leer el archivo evitando proccesos innecesarios (luego cuando se proccessa de nuevo se valida de nuevo) 
        quick_validators = CompositeCountryValidator(validators=[CharacterSpecialPolicy(), CharacterLimitPolicy()])
        quick_validators.ensure_valid(
            content=payload.content,
            rules=rules_country
        )
        await self.forbidden_service.ensure_message_is_valid(payload.content, 4757)
        df = self.df_processor.load_dataframe(payload)
        ordered_tags = extract_tags_with_repeats(payload.content)
        template_polars = re.sub(r"{(\w+(?:-\d+)?)\}", "{}", payload.content)

        # Valida si las etiquetas usadas no contienen palabras prohibidas
        await self.forbidden_service.ensure_dataframe_values_are_valid(
            df,
            ordered_tags,
            4757
        )

        number_column = payload.configFile.nameColumnDemographic
        # START: PROCESO BASE (Logica compartida):
        df = ValidateLevel().execute(df, payload) # Paso 1: Validar nivel de validación
        #df = df.with_columns(pl.lit(True).alias(self.cols.is_ok))
        df = CleanData().execute(df, payload) # Paso 1: Convertir a string y eliminar vacíos y nulos
        df = ConcatPrefix().execute(df, payload) # Paso 2: Concatenar código de país
        df = await AssignOperator(self.number_service).execute(df, payload) # Paso 5: Identificar operador del número
        df = await ExclutionRne(self.rne_service).execute(df, payload) # Paso 3.1: Obtener blacklist como set, Pasar como service con redis
        

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

        
        df = CustomMessage(
            template_polars=template_polars,
            ordered_tags=ordered_tags,
            cols=self.cols
        ).execute(df, payload)
        df = CalculatePDU().execute(df, payload)
        df = await AssignCost(self.cost_service, self.cols).execute(df, payload)
        df = CaculateCredits().execute(df, payload)


        return { 'success': True }
    

        # df =  df.with_columns(
        #     pl.lit('P').alias(self.cols.status)
        # )

        # n = df.height
        # df = df.with_columns(
        #     ((pl.arange(0, n) % 100) + 1).alias(self.cols.service)
        # )

        # cols_save = [
        #     self.cols.number_concat,
        #     self.cols.status, # No la tenemos
        #     self.cols.message,
        #     self.cols.number_operator,
        #     self.cols.pdu,
        #     self.cols.credits,
        #     self.cols.service
        # ]

        # df[cols_save].write_parquet("resultados/test_real.parquet", compression="zstd")


# print(df[['__number_concat__', '__message__', '__credits__', '__pdu_total__', '__is_character_special__', '__length__']].head(10))
        
        # Crear un archivo parquet listo para insercion masiva en la base de datos
         # df.write_parquet("resultados/conoperator.parquet", compression="zstd")
        # FIN: PROCESO BASE:


 # ... proccessos por subservicio (cada uno tiene un proceso diferente)
        # Proccesso Landing 
        # - Valida que el contido tenga una URL
        # - Agrega una url acortada al mensaje
        # - Se debe calcular el costo del mensaje

    # #Filtrar por los que superen el limite de caracteres
        # df_filter = df.filter(pl.col('__length__') > pl.when(pl.col("__is_character_special__")).then(pl.lit(rules_country.limit_character_special)).otherwise(pl.lit(rules_country.limit_character)))
        # # SI hay datos que no cumplen con las reglas del pais, se lanza una excepcion
        # if df_filter.height > 0:
        #     raise ValueError(
        #         "El mensaje supera el límite de caracteres permitido para el país seleccionado." # Devolver el texto y fila
        #     )