from typing import Optional
from pydantic import BaseModel, Field, field_validator


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

    @field_validator("campaignId")
    @classmethod
    def campaign_id_not_empty(cls, v: list[int]) -> list[int]:
        if not v:
            raise ValueError("CAMPAIGN_ID_REQUIRED: campaignId must contain at least one ID.")
        return v
