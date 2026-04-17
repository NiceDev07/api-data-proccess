from typing import Optional
from pydantic import BaseModel, Field


class ConfirmRequest(BaseModel):
    campaignId: list[int] = Field(
        ...,
        description="Lista de IDs de campaña a confirmar para envío.",
        examples=[[229960]],
    )
    codeGroup: Optional[str] = Field(
        None,
        description="Clave de grupo; si se provee, se busca el archivo por codeGroup antes de usar campaignId.",
        examples=["grp_abc123"],
    )
