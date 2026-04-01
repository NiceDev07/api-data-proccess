import polars as pl
from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.constants.cols import Cols
from modules.process.domain.constants.callblasting import OPERATION_MARGIN_SECS

_PPM = 170          # palabras por minuto de locución
_SECS_PER_MIN = 60


class CalculateDurationCustom(IPipeline):
    """Estima la duración de la llamada en segundos a partir del script personalizado.

    Algoritmo por registro:
        1. Cuenta las palabras del mensaje (secuencias no-whitespace).
        2. Calcula duración individual: ceil(word_count / 170 * 60) + margen.
        3. Asigna la duración específica a cada registro.
    """

    _TMP_WORDS = "__cb_word_count__"

    async def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        return (
            df
            .with_columns(
                pl.col(Cols.message)
                .str.count_matches(r"\S+")
                .alias(self._TMP_WORDS)
            )
            .with_columns(
                (
                    (pl.col(self._TMP_WORDS) / pl.lit(_PPM, dtype=pl.Float64) * _SECS_PER_MIN)
                    .ceil()
                    .cast(pl.Int32)
                    + pl.lit(OPERATION_MARGIN_SECS, dtype=pl.Int32)
                ).alias(Cols.seconds)
            )
            .drop(self._TMP_WORDS)
        )
