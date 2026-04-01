from pydantic import BaseModel


# ── SMS ──────────────────────────────────────────────────────────────────────

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


# ── Call Blasting ─────────────────────────────────────────────────────────────

class CBSummaryGroup(BaseModel):
    operator: str
    total: int
    seconds: int
    credits: float
    unit_value: float


class CBSummaryGeneral(BaseModel):
    total_records: int
    total_seconds: int
    total_credits: float
    total_excluded: int


class CBCampaignSummary(BaseModel):
    summaryGroup: list[CBSummaryGroup]
    summaryGeneral: CBSummaryGeneral
