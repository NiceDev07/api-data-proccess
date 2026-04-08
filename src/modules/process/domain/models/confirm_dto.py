from pydantic import BaseModel, Field


class ConfirmRequest(BaseModel):
    campaignId: list[int] = Field(
        ...,
        description="Lista de IDs de campaña a confirmar para envío.",
        examples=[[229960]],
    )
