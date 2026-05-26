import polars as pl
from modules.process.domain.interfaces.audio_duration_provider import IAudioDurationProvider
from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.constants.cols import Cols


class CalculateDurationStandard(IPipeline):
    # Mide la duración del audio vía ffprobe (audioPath) y la asigna a todos los registros.
    # El provider ya incluye el margen de 5s por establecimiento de llamada (add=5 en ffprobe).
    # audioPath es obligatorio para standard — validado en el DTO antes de llegar aquí.

    def __init__(self, duration_provider: IAudioDurationProvider):
        self._provider = duration_provider

    async def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        seconds = await self._provider.get_duration(ctx.audioPath)
        return df.with_columns(pl.lit(max(1, seconds)).cast(pl.Int32).alias(Cols.seconds))
