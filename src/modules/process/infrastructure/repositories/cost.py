from typing import Literal
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from modules.process.infrastructure.models.tariffs import TelCost

ServiceKey = Literal["sms", "email"]          # para get_tariff_costs (SMS / email)
CBServiceKey = Literal["standard", "custom"]  # para get_tariff_costs_cb (call blasting)


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

    async def get_email_cost(
        self,
        country_id: int,
        tariff_id: int,
    ) -> float | None:
        stmt = (
            select(TelCost.email.label("cost"))
            .where(TelCost.country_id == country_id)
            .where(TelCost.tariff_id == tariff_id)
            .limit(1)
        )
        result = await self.session.execute(stmt)
        row = result.scalar_one_or_none()
        return float(row) if row is not None else None

    async def get_tariff_costs_cb(
        self,
        country_id: int,
        tariff_id: int,
        service: CBServiceKey,
    ) -> list[tuple[str, float, str, float, float]]:
        """Igual que get_tariff_costs pero incluye initial e incremental para call blasting."""
        columns_map = {
            "standard": TelCost.cb_standard,
            "custom":   TelCost.cb_custom,
        }

        if service not in columns_map:
            raise ValueError(f"Servicio no soportado para call blasting: {service}")

        cost_col = columns_map[service]
        stmt = (
            select(
                TelCost.prefix,
                cost_col.label("cost"),
                TelCost.operator.label("cost_operator"),
                TelCost.initial,
                TelCost.incremental,
            )
            .where(TelCost.country_id == country_id)
            .where(TelCost.tariff_id == tariff_id)
            .where(cost_col.isnot(None))
            .where(TelCost.initial.isnot(None))
            .where(TelCost.incremental.isnot(None))
        )

        result = await self.session.execute(stmt)
        rows = result.all()
        return sorted(rows, key=lambda x: len(x[0]), reverse=True)
