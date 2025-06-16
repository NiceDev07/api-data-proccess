from modules.campaign_proccess.domain.value_objects.rules_country import RulesCountry
from ..schemas.preload_camp_schema import RulesCountry as RulesCountryDTO

class RulesCountryFactory:
    @staticmethod
    def from_dto(dto: RulesCountryDTO) -> RulesCountry:
        return RulesCountry(
            country_id=dto.idCountry,
            code_country=dto.codeCountry,
            use_character_special=dto.useCharacterSpecial,
            limit_character=dto.limitCharacter,
            limit_character_special=dto.limitCharacterSpecial,
            number_digits_mobile=dto.numberDigitsMobile,
            number_digits_fixed=dto.numberDigitsFixed,
            use_short_name=dto.useShortName
        )
