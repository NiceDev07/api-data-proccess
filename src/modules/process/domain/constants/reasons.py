from typing import Final


class ExclusionReason:
    # Lista de exclusión
    EXCLUSION_LIST: Final[str] = "EXCLUSION_LIST"
    # Número sin operador asignado
    NO_OPERATOR: Final[str] = "NO_OPERATOR"
    # Longitud de número inválida
    INVALID_NUMBER_LENGTH: Final[str] = "INVALID_NUMBER_LENGTH"
    # Email con formato inválido
    INVALID_EMAIL: Final[str] = "INVALID_EMAIL"
    # Sin tarifa configurada para el número (call blasting)
    NO_COST: Final[str] = "NO_COST"
    # Violaciones de regulación SMS
    SHORTNAME_MISSING: Final[str] = "SHORTNAME_MISSING"
    SPECIAL_CHAR_NOT_ALLOWED: Final[str] = "SPECIAL_CHAR_NOT_ALLOWED"
    # Mensaje sin URL en el sub-servicio landing
    URL_REQUIRED: Final[str] = "URL_REQUIRED"
    # CHAR_LIMIT_EXCEEDED — desactivado: mensajes largos se cobran como multi-parte.
    # CHAR_LIMIT_EXCEEDED: Final[str] = "CHAR_LIMIT_EXCEEDED"
