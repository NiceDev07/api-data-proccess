# src/domain/entities/rules_country.py

from dataclasses import dataclass

@dataclass(frozen=True)
class RulesCountry:
    country_id: int              # Identificador del país
    code_country: int            # Código del país
    use_character_special: bool           # ¿Puede enviar caracteres especiales?
    limit_character: int                  # Límite sin caracteres especiales
    limit_character_special: int          # Límite con caracteres especiales
    number_digits_mobile: int             # Longitud de números móviles
    number_digits_fixed: int              # Longitud de números fijos
    use_short_name: bool                  # ¿Requiere nombre corto o identificador?

    def is_valid_length(self, message: str) -> bool:
        if self.contains_special_characters(message):
            return len(message) <= self.limit_character_special
        return len(message) <= self.limit_character
    
    def can_send_special_characters(self, message: str) -> bool:
        return not self.use_character_special and self.contains_special_characters(message)

    def contains_special_characters(self, message: str) -> bool:
        length = len(message)
        length_bytes = len(message.encode('utf-8'))
        return length != length_bytes

    def requires_identifier(self) -> bool:
        return self.use_short_name
