from modules.campaign_proccess.domain.interfaces.rule_policies import ICountryRulePolicy
from modules.campaign_proccess.domain.value_objects.rules_country import RulesCountry

class CharacterSpecialPolicy(ICountryRulePolicy):
    def validate(self, content: str, rules: RulesCountry) -> bool:
        if not rules.use_character_special:
            return not rules.contains_special_characters(content)

        return rules.use_character_special 

    def error_message(self) -> str:
        return "El mensaje contiene caracteres especiales no permitidos para este país."

class CharacterLimitPolicy(ICountryRulePolicy):
    def validate(self, content: str, rules: RulesCountry) -> bool:
        if rules.contains_special_characters(content):
            return len(content) <= rules.limit_character_special
        return len(content) <= rules.limit_character

    def error_message(self) -> str:
        return "El mensaje excede el límite de caracteres permitido para este país."

class ShortNamePolicy(ICountryRulePolicy):
    def validate(self, content: str, rules: RulesCountry) -> bool:
        if not rules.use_short_name:
            return True
        return "{name}" in content  # o cualquier lógica más robusta

    def error_message(self) -> str:
        return "El identificador corto es obligatorio en el contenido."