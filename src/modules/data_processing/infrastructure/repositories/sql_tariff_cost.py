from sqlalchemy.orm import Session
from modules.data_processing.domain.interfaces.tariff_repository import ICostRepository
from modules.data_processing.infrastructure.models.tariff_cost import CostTable
from typing import List, Tuple
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

class CostRepository(ICostRepository):
    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_tariff_costs(
        self,
        country_id: int,
        tariff_id: int,
        service: str
    ) -> List[Tuple[str, float, str]]:
        columns_map = {
            "sms": CostTable.sms,
            "call_blasting_standard": CostTable.cb_standard,
            "call_blasting_custom": CostTable.cb_custom,
            "email": CostTable.email,
        }

        if service not in columns_map:
            raise ValueError(f"Servicio desconocido: {service}")

        stmt = (
            select(
                CostTable.prefix,
                columns_map[service].label("cost"),
                CostTable.operator.label("cost_operator")
            )
            .where(CostTable.country_id == country_id)
            .where(CostTable.tariff_id == tariff_id)
        )

        result = await self.db.execute(stmt)
        rows = result.all()  # lista de tuplas (prefix, cost, cost_operator)

        # Ordenar por longitud del prefijo (desc)
        return sorted(rows, key=lambda x: len(x[0]), reverse=True)