from pydantic import BaseModel


class SummaryGroup(BaseModel):
    operator: str
    total: int
    pdu: int
    credits: float
    unit_value: float  # costo promedio por registro (credits / total)


class SummaryGeneral(BaseModel):
    total_records: int
    total_pdu: int
    total_credits: float
    total_excluded: int


class CampaignSummary(BaseModel):
    summaryGroup: list[SummaryGroup]
    summaryGeneral: SummaryGeneral
