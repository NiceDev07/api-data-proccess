from typing import List
from ..interfaces.rule_policies import ICountryRulePolicy
from ..value_objects.rules_country import RulesCountry

class CompositeCountryValidator(ICountryRulePolicy):
    def __init__(self, validators: List[ICountryRulePolicy]):
        self._validators = validators
        self._errors = []
    
    def validate(self, content: str, rules: RulesCountry) -> bool:
        self._errors = []
        for validator in self._validators:
            if not validator.validate(content, rules):
                self._errors.append(validator.error_message())
        return len(self._errors) == 0
    
    def error_message(self) -> str:
        return "\n".join(self._errors)