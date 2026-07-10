from typing import List, Tuple
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from ..models.numeration import Numeracion
from modules.process.domain.interfaces.numeracion_repository import INumeracionRepository
from logging_config import get_logger

logger = get_logger(__name__)


class NumeracionRepository(INumeracionRepository):
    def __init__(self, db_numeracion: AsyncSession):
        self.db_connection = db_numeracion

    async def get_numeracion(self, country_id: int) -> List[Tuple[int, int, str]]:
        try:
            stmt = (
                select(
                    Numeracion.inicio,
                    Numeracion.fin,
                    Numeracion.operador,
                )
                .where(Numeracion.id_pais == country_id)
                .order_by(Numeracion.inicio.asc())
            )
            result = await self.db_connection.execute(stmt)
            rows = result.all()

            if not rows:
                logger.error("Sin datos de numeración para country_id=%s", country_id)
                raise RuntimeError(f"numeration_not_found: no hay rangos configurados para el país {country_id}")

            return rows
        except RuntimeError:
            raise
        except Exception as e:
            await self.db_connection.rollback()
            logger.exception("Error al consultar numeración para country_id=%s", country_id)
            raise RuntimeError("numeration_query_failed") from e