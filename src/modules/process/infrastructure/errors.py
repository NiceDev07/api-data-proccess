"""
Helper centralizado para construir el `detail` de las respuestas de error HTTP.

Convierte mensajes en formato "CODIGO: descripción" (estándar del proyecto)
a `{"code": "CODIGO", "message": "descripción"}` para que el gateway/frontend
no tenga que parsear strings.
"""
from typing import Dict


def build_error_detail(message: str) -> Dict[str, str]:
    # Parsea "CODE: msg" devolviendo {code, message}.
    # Si el mensaje no tiene el patrón CODE:, se devuelve UNKNOWN como código.
    if ": " in message:
        code, _, msg = message.partition(": ")
        return {"code": code, "message": msg}
    return {"code": "UNKNOWN", "message": message}
