from pydantic import BaseModel


# ── Violaciones de regulación ─────────────────────────────────────────────────

class RegulationViolation(BaseModel):
    code: str
    affected: int
    description: str


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
    violations: list[RegulationViolation] = []


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


# ── Email ─────────────────────────────────────────────────────────────────────

class EmailSummaryGroup(BaseModel):
    domain: str
    total: int
    total_excluded: int
    unit_value: float   # costo por registro
    credits: float


class EmailSummaryGeneral(BaseModel):
    total_records: int
    total_excluded: int  # excluidos por lista + registros inválidos (email mal formado)
    total_credits: float


class EmailCampaignSummary(BaseModel):
    summaryGroup: list[EmailSummaryGroup]
    summaryGeneral: EmailSummaryGeneral
