import math
import polars as pl
from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.constants.cols import Cols
from modules.process.domain.constants.callblasting import OPERATION_MARGIN_SECS

_PPM = 170          # palabras por minuto de locución
_SECS_PER_MIN = 60


class CalculateDurationCustom(IPipeline):
    """Estima la duración de la llamada en segundos a partir del script personalizado.

    Algoritmo:
        1. Cuenta las palabras de cada mensaje (secuencias no-whitespace).
        2. Estima la duración individual: word_count / 170 * 60 segundos.
        3. Calcula el promedio de duración sobre los registros válidos.
        4. Asigna PDU = ceil(promedio) a todos los registros (mismo valor por campaña).
    """

    _TMP_WORDS = "__cb_word_count__"
    _TMP_DUR   = "__cb_duration__"

    async def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        df = df.with_columns(
            pl.col(Cols.message)
            .str.count_matches(r"\S+")
            .alias(self._TMP_WORDS)
        ).with_columns(
            (pl.col(self._TMP_WORDS) / pl.lit(_PPM, dtype=pl.Float64) * _SECS_PER_MIN)
            .alias(self._TMP_DUR)
        )

        # Promedia solo sobre registros válidos para no distorsionar con excluidos
        valid = df.filter(pl.col(Cols.is_ok))
        avg = valid[self._TMP_DUR].mean() if not valid.is_empty() else 0.0
        duration = max(1, math.ceil(avg or 0.0)) + OPERATION_MARGIN_SECS

        return df.drop([self._TMP_WORDS, self._TMP_DUR]).with_columns(
            pl.lit(duration).cast(pl.Int32).alias(Cols.seconds)
        )
