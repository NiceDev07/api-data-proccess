from sqlalchemy.orm import Session
from modules.data_processing.domain.interfaces.tariff_repository import ICostRepository
from modules.data_processing.infrastructure.models.tariff_cost import CostTable


class CostRepository(ICostRepository):
    def __init__(self, db: Session):
        self.db = db

    def get_tariff_costs(self, country_id: int, tariff_id: int, service: str) -> list[tuple[str, float]]:
        columns_map = {
            'sms': CostTable.sms,
            'call_blasting_standard': CostTable.cb_standard,
            'call_blasting_custom': CostTable.cb_custom,
            'email': CostTable.email
        }

        if service not in columns_map:
            raise ValueError(f"Servicio desconocido: {service}")

        results = (
            self.db.query(
                CostTable.prefix,
                columns_map[service].label("cost"),
                CostTable.operator.label("cost_operator")
            )
            .filter(CostTable.country_id == country_id)
            .filter(CostTable.tariff_id == tariff_id)
            .all()
        )

        return sorted(results, key=lambda x: len(x[0]), reverse=True)  # Orden por longitud de prefijo DESC