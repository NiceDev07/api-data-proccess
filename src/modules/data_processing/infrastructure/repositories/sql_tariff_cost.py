from sqlalchemy.orm import Session
from modules.data_processing.domain.interfaces.tariff_repository import ICostRepository
from modules.data_processing.domain.value_objects.tel_cost_info import TelcoCostInfo
from modules.data_processing.infrastructure.models.tariff_cost import CostTable
import pandas as pd
from datetime import datetime

class CostRepository(ICostRepository):
    def __init__(self, db: Session):
        self.db = db

    def get_tariff_cost_data(self, country_id: int, tariff_id: int, service: str) -> TelcoCostInfo:
        columns_map = {
            'sms': CostTable.sms,
            'call_blasting_standard': CostTable.cb_standard,
            'call_blasting_custom': CostTable.cb_custom,
            'email': CostTable.email
        }
        if service not in columns_map:
            raise ValueError(f"Servicio desconocido: {service}")

        query = (
            self.db.query(
                CostTable.prefix,
                columns_map[service].label("cost"),
                CostTable.initial,
                CostTable.incremental
            )
            .filter(CostTable.country_id == country_id)
            .filter(CostTable.tariff_id == tariff_id)
        )
        results = query.all()

        if not results:
            raise ValueError("No se encontraron datos de tarifas para el país y tarifa especificados.")# Cambiar a error específico si es necesario

        df = pd.DataFrame(results, columns=["prefix", "cost", "initial", "incremental"])
        df["_prefix_length"] = df["prefix"].str.len()
        df = df.sort_values(by="_prefix_length", ascending=False).reset_index(drop=True)

        return TelcoCostInfo(
            prefixes=df["prefix"].to_numpy(dtype=str),
            costs=df["cost"].to_numpy(dtype=float),
            lengths=df["_prefix_length"].to_numpy(dtype=int),
            initial=df["initial"].to_numpy(dtype=float),
            incremental=df["incremental"].to_numpy(dtype=float),
            date_cache=datetime.now()
        )
