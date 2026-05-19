from pydantic import BaseModel, Field


class ConfirmRequest(BaseModel):
    campaignId: list[int] = Field(
        default_factory=list,
        description="IDs de campaña. Opcional cuando se envía codeGroup.",
        examples=[[229960]],
    )
    codeGroup: str = Field(
        ...,
        description="Identificador de grupo para localizar el Parquet. Tiene prioridad sobre campaignId.",
        examples=["grp_abc123"],
    )
