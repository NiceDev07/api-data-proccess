from typing import List, Optional
from pydantic import BaseModel, Field
from modules.process.domain.models.process_dto import RulesCountry, InfoUserValidSend


class InlineSmsRequest(BaseModel):
    """Payload del flujo UNITARIO: los números viajan en el request (sin archivo).
    Reutiliza el mismo pipeline SMS que el masivo — solo cambia la fuente de datos."""
    content: str = Field(..., description="Plantilla del mensaje SMS.")
    shortname: Optional[str] = Field(None, description="Remitente; requerido si el país usa shortname.")
    tariffId: int = Field(..., description="ID de tarifa para el cálculo de costo/créditos.")
    subService: str = Field("informative", description="SMS: 'informative' | 'landing'.")
    rulesCountry: RulesCountry = Field(..., description="Reglas del país (prefijo, dígitos, límites).")
    infoUserValidSend: InfoUserValidSend = Field(..., description="Nivel del usuario + demográfico.")
    useFlash: bool = Field(default=False, description="SMS flash: selecciona el código corto flash en el routing (SP).")
    gCode: int = Field(default=1, description="Grupo de códigos cortos para el routing (SP).")
    # Tope explícito: el unitario resuelve el operador con 1 CALL al SP por número
    # (secuencial). Un tope acotado evita degradar el servicio con envíos masivos por
    # este endpoint (para volumen alto existe el flujo masivo por archivo).
    numbers: List[int] = Field(..., min_length=1, max_length=1000, description="Números nacionales o con prefijo (máx. 1000).")
