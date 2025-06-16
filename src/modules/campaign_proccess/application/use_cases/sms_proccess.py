from core.file.interfaces.file_reader import IFileReader
from modules.campaign_proccess.application.schemas.preload_camp_schema import PreloadCampDTO
from modules.campaign_proccess.application.helpers.tags import extract_tags_from_content
from modules.campaign_proccess.application.helpers.required_columns import build_required_columns
from modules.campaign_proccess.domain.value_objects.rules_country import RulesCountry
from modules.campaign_proccess.domain.policies.composition import CompositeCountryValidator
from modules.campaign_proccess.domain.policies.validate_policies import (CharacterSpecialPolicy, CharacterLimitPolicy)
from modules.campaign_proccess.domain.value_objects.rules_country import RulesCountry
from modules.campaign_proccess.domain.interfaces.forbidden_words_repository import IForbiddenWordsRepository
from modules.campaign_proccess.domain.services.forbbiden_works import ForbiddenWordsService

class SMSUseCase:
    def __init__(
        self,
        file_reader: IFileReader,
        rules_country: RulesCountry,  # Uncomment if you want to use rules_country in the future
        forbidden_service: ForbiddenWordsService
    ):
        self.file_reader = file_reader
        self.rules_country = rules_country  # Uncomment if you want to use rules_country in the future
        self.forbidden_service = forbidden_service  # Optional, if you want to use forbidden words in the future

    def execute(self, payload: PreloadCampDTO): 
        # Se hace una primera validacion del mensaje base para determinar si cumple con las reglas del pais
        # Asi si no cumple, no se procede a leer el archivo evitando proccesos innecesarios (luego cuando se proccessa de nuevo se valida de nuevo) 
        quick_validators = CompositeCountryValidator(validators=[CharacterSpecialPolicy(), CharacterLimitPolicy()])
        is_valid = quick_validators.validate(payload.content, self.rules_country)
        if not is_valid:
            raise ValueError(quick_validators.error_message())
         
        # Validar si el mensaje base tiene palabras no autorizadas
        self.forbidden_service.ensure_message_is_valid(payload.content, 4757)
        
        file_path = payload.configFile.folder
        content_tags = extract_tags_from_content(payload.content)
        usecols = build_required_columns(list(content_tags), payload.configFile)
        
        df = self.file_reader.read(file_path, usecols=usecols)
        
        # START: PROCESO BASE:
        # Limpiar el DataFrame eliminando todos los datos que esten vacios en la columna de numero de telefono (guardar este dato, cuantos se eliminaron)
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