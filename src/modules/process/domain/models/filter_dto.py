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


class ValidateCampaignRequest(BaseModel):
    mensajes: list[str]
    user_id: int


class BlockedMessage(BaseModel):
    indice: int
    mensaje: str
    palabra: str


class ValidateCampaignResponse(BaseModel):
    total: int
    aprobados: int
    bloqueados: list[BlockedMessage]


class InvalidateCacheRequest(BaseModel):
    secret: str


class InvalidateCacheResponse(BaseModel):
    invalidado: bool
