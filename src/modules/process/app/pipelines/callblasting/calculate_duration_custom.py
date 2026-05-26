import polars as pl
from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.constants.cols import Cols

_PPM            = 170   # palabras por minuto de locución
_SECS_PER_MIN   = 60
_MARGIN_SECS    = 7     # margen operativo añadido a la estimación TTS

_COMPACT_MIN_WORDS = 5    # umbral: si hay menos palabras que esto…
_COMPACT_MIN_CHARS = 100  # …y más caracteres que esto → texto compacto sin espacios
_CHARS_PER_WORD    = 5    # estimación: ~1 palabra cada 5 caracteres


class CalculateDurationCustom(IPipeline):
    """Estima la duración de la llamada en segundos a partir del script personalizado.

    Algoritmo por registro:
        1. Cuenta palabras del mensaje (secuencias no-whitespace).
        2. Fallback texto compacto: si hay < 5 palabras pero > 100 caracteres,
           estima palabras como max(conteo, len // 5).
        3. Calcula duración: ceil(word_count / 170 * 60 + 7).
        4. Asigna la duración específica a cada registro.
    """

    _TMP_WORDS = "__cb_word_count__"

    async def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        word_count = pl.col(Cols.message).str.count_matches(r"\S+")
        text_len   = pl.col(Cols.message).str.len_chars()

        # Fallback: texto sin espacios (URL, código, texto pegado)
        adjusted_words = (
            pl.when((word_count < _COMPACT_MIN_WORDS) & (text_len > _COMPACT_MIN_CHARS))
            .then(pl.max_horizontal(word_count, (text_len / _CHARS_PER_WORD).cast(pl.Int32)))
            .otherwise(word_count)
        )

        return (
            df
            .with_columns(adjusted_words.alias(self._TMP_WORDS))
            .with_columns(
                (
                    pl.col(self._TMP_WORDS) / pl.lit(_PPM, dtype=pl.Float64) * _SECS_PER_MIN
                    + pl.lit(_MARGIN_SECS, dtype=pl.Float64)
                )
                .ceil()
                .cast(pl.Int32)
                .alias(Cols.seconds)
            )
            .drop(self._TMP_WORDS)
        )
