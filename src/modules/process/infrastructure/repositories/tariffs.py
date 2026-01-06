from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

class TariffsRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def fetch(
        self,
        country_id: int,
        cols: list[str],
        limit_one: bool = False,
    ) -> list[dict]:
        from models import Tariff  # o inyectarlo si prefieres

        select_cols = [
            Tariff.country_id,
            Tariff.prefix,
            Tariff.operator,
            *[getattr(Tariff, c) for c in cols],
        ]

        stmt = select(*select_cols).where(Tariff.country_id == country_id)

        if limit_one:
            stmt = stmt.order_by(
                func.length(Tariff.prefix).desc(),
                (Tariff.operator != "all").desc(),
            ).limit(1)

        rows = (await self.session.execute(stmt)).all()
        if not rows:
            return []

        names = ["country_id", "prefix", "operator", *cols]
        return [dict(zip(names, r)) for r in rows]
