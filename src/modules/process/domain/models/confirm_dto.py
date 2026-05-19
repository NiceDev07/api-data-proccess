from pydantic import BaseModel, Field


class ConfirmRequest(BaseModel):
    campaignId: list[int] = Field(
        ...,
        min_length=1,
        description="IDs de campaña. Requerido con al menos un elemento.",
        examples=[[229960]],
    )
    codeGroup: str = Field(
        ...,
        description="Identificador de grupo para localizar el Parquet. Tiene prioridad sobre campaignId.",
        examples=["KXQM7291"],
    )
