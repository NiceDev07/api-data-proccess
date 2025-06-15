from core.file.interfaces.file_reader import IFileReader
from modules.campaign_proccess.application.schemas.preload_camp_schema import PreloadCampDTO
from modules.campaign_proccess.application.helpers.tags import extract_tags_from_content
from modules.campaign_proccess.application.helpers.required_columns import build_required_columns
from modules.campaign_proccess.domain.entities.rules_country import RulesCountry
from modules.campaign_proccess.domain.policies.composition import CompositeCountryValidator
from modules.campaign_proccess.domain.policies.validate_policies import (CharacterSpecialPolicy, CharacterLimitPolicy)

class SMSUseCase:
    def __init__(
        self,
        file_reader: IFileReader
    ):
        self.file_reader = file_reader

    def execute(self, payload: PreloadCampDTO):
        rules_country = RulesCountry(
            country_id=payload.rulesCountry.idCountry,
            code_country=payload.rulesCountry.codeCountry,
            use_character_special=payload.rulesCountry.useCharacterSpecial,
            limit_character=payload.rulesCountry.limitCharacter,
            limit_character_special=payload.rulesCountry.limitCharacterSpecial,
            number_digits_mobile=payload.rulesCountry.numberDigitsMobile,
            number_digits_fixed=payload.rulesCountry.numberDigitsFixed,
            use_short_name=payload.rulesCountry.useShortName
        )
        # Se hace una primera validacion del mensaje base para determinar si cumple con las reglas del pais
        # Asi si no cumple, no se procede a leer el archivo evitando proccesos innecesarios (luego cuando se proccessa de nuevo se valida de nuevo) 
        validations = CompositeCountryValidator(validators=[CharacterSpecialPolicy(), CharacterLimitPolicy()])
        is_valid = validations.validate(payload.content, rules_country)
        if not is_valid:
            raise ValueError(validations.error_message())
        
        # Validar si el mensaje base tiene palabras no autorizadas

        file_path = payload.configFile.folder
        content_tags = extract_tags_from_content(payload.content)
        usecols = build_required_columns(list(content_tags), payload.configFile)
        
        df = self.file_reader.read(file_path, usecols=usecols)
        # Validar si el mensaje base no contenga ninguna palabra no autorizada
        # START: PROCESO BASE:
        # Limpiar el DataFrame eliminando todos los datos que esten vacios en la columna de numero de telefono (guardar este dato, cuantos se eliminaron)
        # Cruce de datos con la lista de exclusion (del usuario si lo requiere)
        # Cruce con la lista de exclusion general (si aplica)
        # Cruce para identificar el operador del numero 
        # Personalizacion de mensaje 
        # Calculo de costo del mensaje
        # FIN: PROCESO BASE:
        
        # ... proccessos por subservicio (cada uno tiene un proceso diferente)
        # Proccesso Landing 
        # - Valida que el contido tenga una URL
        # - Agrega una url acortada al mensaje
        # - Se debe calcular el costo del mensaje

        return { 'success': True }