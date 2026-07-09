from typing import Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from modules.process.domain.interfaces.portability_repository import IPortabilityRepository
from logging_config import get_logger

logger = get_logger(__name__)


class PortabilityRepository(IPortabilityRepository):
    """Resuelve operador + routing + PORTABILIDAD de UN número vía el SP
    `portabilidad.consulta_operador_pais` (que hace JOIN a `masivos_sms`).

    Solo para el flujo UNITARIO (pocos números): una llamada por número es
    aceptable. El masivo NO usa esto — resuelve operador con numeración
    vectorizada (millones de filas) donde un CALL por fila no escalaría.

    SP (5 args): consulta_operador_pais(in_celular, in_codeFlash, in_gCode,
                                        in_national_len, in_id_pais)
    (SP nuevo, aparte del original consulta_operador de 3 args que queda intacto.)
    Devuelve por número: codigo, codigo_corto, operador, tags, usuario_api,
    servidor, id_operador. En error responde codigo_corto='99999'.
    """

    def __init__(self, db_portabilidad: AsyncSession):
        self.db = db_portabilidad

    async def consulta_operador(
        self,
        celular: str,
        code_flash: str,
        gcode: int,
        national_len: int,
        id_pais: int,
    ) -> Optional[dict]:
        try:
            result = await self.db.execute(
                text(
                    "CALL portabilidad.consulta_operador_pais"
                    "(:cel, :flash, :gcode, :nlen, :pais)"
                ),
                {
                    "cel": celular,
                    "flash": code_flash,
                    "gcode": gcode,
                    "nlen": national_len,
                    "pais": id_pais,
                },
            )
            # Consumir COMPLETO el result-set del SP evita 'commands out of sync'
            # en las llamadas secuenciales por número sobre la misma conexión.
            rows = result.mappings().all()
            return dict(rows[0]) if rows else None
        except Exception as exc:
            await self.db.rollback()
            logger.exception(
                "Error en consulta_operador | celular=%s | id_pais=%s", celular, id_pais
            )
            raise RuntimeError("portability_query_failed") from exc
