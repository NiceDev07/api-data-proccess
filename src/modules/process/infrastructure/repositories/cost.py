from typing import Literal
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from modules.process.infrastructure.models.tariffs import TelCost

ServiceKey = Literal["sms", "call_blasting_standard", "call_blasting_custom", "email"]


class CostRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_tariff_costs(
        self,
        country_id: int,
        tariff_id: int,
        service: ServiceKey,
    ) -> list[tuple[str, float, str]]:
        columns_map = {
            "sms": TelCost.sms,
            "call_blasting_standard": TelCost.cb_standard,
            "call_blasting_custom": TelCost.cb_custom,
            "email": TelCost.email,
        }

        if service not in columns_map:
            raise ValueError(f"Servicio desconocido: {service}")

        stmt = (
            select(
                TelCost.prefix,
                columns_map[service].label("cost"),
                TelCost.operator.label("cost_operator"),
            )
            .where(TelCost.country_id == country_id)
            .where(TelCost.tariff_id == tariff_id)
        )

        result = await self.session.execute(stmt)
        rows = result.all()
        return sorted(rows, key=lambda x: len(x[0]), reverse=True)
