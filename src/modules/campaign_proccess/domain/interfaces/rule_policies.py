from abc import ABC, abstractmethod
from modules.campaign_proccess.domain.value_objects.rules_country import RulesCountry

class ICountryRulePolicy(ABC):
    @abstractmethod
    def validate(self, content: str, rules: RulesCountry) -> bool:
        pass

    @abstractmethod
    def error_message(self) -> str:
        pass
