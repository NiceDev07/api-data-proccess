from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from modules.process.domain.interfaces.forbidden_words_repository import IForbiddenWordsRepository


_QUERY = text(
    """
    SELECT f.termino, e.user_id
    FROM filtro_sms f
    LEFT JOIN filtro_sms_excepciones e ON e.filtro_id = f.id
    WHERE f.activo = 1
    """
)


class ForbiddenWordsRepository(IForbiddenWordsRepository):
    """
    Repositorio que crea su propia sesión a partir del session_factory.
    Esto permite que sea construido una sola vez en el lifespan y usado
    por el ForbiddenWordsService sin depender de una sesión por-request.
    """

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self._factory = session_factory

    async def get_active_terms(self) -> dict[str, set[int]]:
        async with self._factory() as session:
            result = await session.execute(_QUERY)
            rows = result.fetchall()

        grouped: dict[str, set[int]] = {}
        for termino, user_id in rows:
            bucket = grouped.setdefault(termino, set())
            if user_id is not None:
                bucket.add(int(user_id))

        return grouped
