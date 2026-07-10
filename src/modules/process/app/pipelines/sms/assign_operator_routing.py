import polars as pl

from modules.process.domain.constants.cols import Cols
from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.domain.interfaces.portability_repository import IPortabilityRepository
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.app.pipelines.sms._operator_common import DEFAULT_OPERATOR, mark_no_operator

# codigo_corto que el SP devuelve cuando NO resuelve operador/routing (rama de error).
_ERROR_SHORT_CODES = {"99999", "99998"}
# Centinela de operador de la rama de error del SP ('ERROR'). Case-insensitive por robustez.
_ERROR_OPERATOR = "ERROR"
# code_flash que espera el SP para SMS normal vs flash.
_CODE_FLASH_NORMAL = "saem1"
_CODE_FLASH_FLASH = "saem2"


def _is_error_operator(operator) -> bool:
    return operator is None or (isinstance(operator, str) and operator.strip().upper() in {"", _ERROR_OPERATOR})


class AssignOperatorRouting(IPipeline):
    """UNITARIO: resuelve operador + routing + PORTABILIDAD por número vía el SP
    `consulta_operador_pais`. Reemplaza a `AssignOperator` (numeración vectorizada)
    en el flujo unitario.

    - El masivo NO usa este paso (un CALL por fila no escala a millones).
    - A diferencia de `AssignOperator`, aquí SÍ hay portabilidad (el SP consulta
      la tabla de portados) y se resuelve el routing de envío (código corto, API,
      servidor) de una sola vez → el MSA solo persiste.
    - El costo NO depende del operador (AssignCost es por prefijo), así que swappear
      este paso no altera costo/PDU/créditos.

    Fail-closed: si el SP no resuelve (codigo_corto de error u operador centinela),
    el número se invalida con NO_OPERATOR y no se envía.
    """

    default_operator = DEFAULT_OPERATOR

    def __init__(self, portability_repo: IPortabilityRepository):
        self.portability_repo = portability_repo

    async def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        phone_col = ctx.configFile.nameColumnDemographic
        prefix = ctx.rulesCountry.codeCountry
        # national_digits (= max(mobile, fixed)) es el mismo criterio que usan
        # CleanData/ConcatPrefix/LevelValidator para "número nacional" — antes este
        # paso usaba solo numberDigitsMobile, que diverge si un país tiene fixed>mobile.
        national_len = ctx.rulesCountry.national_digits
        id_pais = ctx.rulesCountry.idCountry
        code_flash = _CODE_FLASH_FLASH if ctx.useFlash else _CODE_FLASH_NORMAL
        gcode = ctx.gCode

        df = df.with_columns(pl.col(phone_col).cast(pl.Int64))

        operators: list[str] = []
        short_codes: list = []
        user_apis: list = []
        servers: list = []
        operator_ids: list[int] = []
        resolved: list[bool] = []

        for national in df[phone_col].to_list():
            info = None
            if national is not None:
                celular = f"{prefix}{national}"
                info = await self.portability_repo.consulta_operador(
                    celular, code_flash, gcode, national_len, id_pais
                )

            short = str(info["codigo_corto"]) if info and info.get("codigo_corto") is not None else None
            operator = info.get("operador") if info else None
            # `short is None` cubre el caso donde el SP resuelve operador pero no trae
            # routing completo (código corto/usuario_api/servidor nulos) — sin esto la
            # fila quedaría marcada resolved=True con routing inutilizable.
            is_error = (
                info is None
                or short is None
                or short in _ERROR_SHORT_CODES
                or _is_error_operator(operator)
                or not info.get("usuario_api")
                or not info.get("servidor")
            )

            if is_error:
                operators.append(self.default_operator)
                short_codes.append(None)
                user_apis.append(None)
                servers.append(None)
                operator_ids.append(0)
                resolved.append(False)
            else:
                operators.append(operator)
                short_codes.append(short)
                user_apis.append(info.get("usuario_api"))
                servers.append(info.get("servidor"))
                operator_ids.append(int(info.get("id_operador") or 0))
                resolved.append(True)

        df = df.with_columns(
            pl.Series(Cols.number_operator, operators, dtype=pl.String),
            pl.Series(Cols.short_code, short_codes, dtype=pl.String),
            pl.Series(Cols.user_api, user_apis, dtype=pl.String),
            pl.Series(Cols.server, servers, dtype=pl.String),
            pl.Series(Cols.operator_id, operator_ids, dtype=pl.Int64),
        )
        # Invalidación NO_OPERATOR compartida con AssignOperator (no pisa errores previos).
        return mark_no_operator(df, pl.Series(resolved))
