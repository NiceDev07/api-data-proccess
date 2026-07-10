from sqlalchemy import Integer, String, ForeignKey, TIMESTAMP, func
from sqlalchemy.orm import Mapped, mapped_column
from .base import Base


class FiltroSms(Base):
    __tablename__ = "filtro_sms"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True, unsigned=True)
    termino: Mapped[str] = mapped_column(String(191), nullable=False, unique=True)
    perfil: Mapped[str | None] = mapped_column(String(100), nullable=True)
    activo: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    created_at: Mapped[object] = mapped_column(TIMESTAMP, server_default=func.now(), nullable=False)


class FiltroSmsExcepcion(Base):
    __tablename__ = "filtro_sms_excepciones"

    filtro_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("filtro_sms.id", ondelete="CASCADE"),
        primary_key=True,
        unsigned=True,
    )
    user_id: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False, unsigned=True)
    created_at: Mapped[object] = mapped_column(TIMESTAMP, server_default=func.now(), nullable=False)
