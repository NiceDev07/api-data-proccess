from typing import Optional
from pydantic import BaseModel, Field, model_validator


class ConfirmRequest(BaseModel):
    campaignId: list[int] = Field(
        default_factory=list,
        description="Lista de IDs de campaña a confirmar para envío.",
        examples=[[229960]],
    )
    codeGroup: Optional[str] = Field(
        None,
        description="Clave de grupo; si se provee, se busca el archivo por codeGroup antes de usar campaignId.",
        examples=["grp_abc123"],
    )

    @model_validator(mode="after")
    def campaign_id_or_code_group(self) -> "ConfirmRequest":
        if not self.campaignId and not self.codeGroup:
            raise ValueError("CAMPAIGN_ID_REQUIRED: Provide campaignId or codeGroup.")
        return self
