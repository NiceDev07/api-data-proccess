from pydantic import BaseModel


class ValidateTextRequest(BaseModel):
    texto: str
    user_id: int


class BlockedWord(BaseModel):
    palabra: str
    posicion: int


class ValidateTextResponse(BaseModel):
    permitido: bool
    palabras_bloqueadas: list[BlockedWord]
