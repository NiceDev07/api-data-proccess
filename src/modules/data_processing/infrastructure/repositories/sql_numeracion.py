from sqlalchemy.orm import Session
from typing import List, Tuple

from modules.data_processing.infrastructure.models.numeracion import Numeracion
from modules.data_processing.domain.interfaces.numeracion_repository import INumeracionRepository

class NumeracionRepository(INumeracionRepository):
    def __init__(self, db_numeracioon: Session):
        self.db_connection = db_numeracioon

    def get_numeracion(self, country_id: int) -> List[Tuple[int, int, str]]:
        result = (
            self.db_connection.query(
                Numeracion.inicio,
                Numeracion.fin,
                Numeracion.operador
            )
            .filter(Numeracion.id_pais == country_id)
            .order_by(Numeracion.inicio.asc())
            .all()
        )
        return result
