from sqlalchemy import (
    Integer,
    String,
    Enum,
    ForeignKey,
    DECIMAL,
)
from sqlalchemy.orm import Mapped, mapped_column
from .base import Base

class TelCost(Base):
    __tablename__ = "w48fa_tel_cost"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    type: Mapped[str] = mapped_column(
        Enum("mobile", "fixed", name="tel_cost_type"),
        nullable=False,
    )

    prefix: Mapped[str] = mapped_column(String(20), nullable=False)

    operator: Mapped[str] = mapped_column(String(100), nullable=False)

    sms: Mapped[float] = mapped_column(DECIMAL(10, 4), nullable=False)

    cb_standard: Mapped[float] = mapped_column(DECIMAL(10, 4), nullable=False)

    cb_custom: Mapped[float] = mapped_column(DECIMAL(10, 4), nullable=False)

    email: Mapped[float] = mapped_column(DECIMAL(10, 4), nullable=False)

    initial: Mapped[float] = mapped_column(DECIMAL(10, 4), nullable=False)

    incremental: Mapped[float] = mapped_column(DECIMAL(10, 4), nullable=False)

    tariff_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("w48fa_tariffs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    country_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
