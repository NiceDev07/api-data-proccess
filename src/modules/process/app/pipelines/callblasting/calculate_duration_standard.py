import math

import polars as pl

from modules.process.domain.interfaces.audio_duration_provider import IAudioDurationProvider
from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.constants.cols import Cols
from modules.process.domain.constants.callblasting import OPERATION_MARGIN_SECS


class CalculateDurationStandard(IPipeline):
    # Asigna la duración del audio (en segundos) a todos los registros.
    # Prioridad: audioDuration (ya calculada) > audioPath (se delega al provider).

    def __init__(self, duration_provider: IAudioDurationProvider):
        self._provider = duration_provider

    async def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        if ctx.audioDuration is not None:
            seconds = ctx.audioDuration
        elif ctx.audioPath is not None:
            seconds = await self._provider.get_duration(ctx.audioPath)
        else:
            raise ValueError(
                "AUDIO_SOURCE_REQUIRED: 'audioDuration' or 'audioPath' is required for call_blasting standard."
            )

        duration = max(1, math.ceil(seconds)) + OPERATION_MARGIN_SECS
        return df.with_columns(pl.lit(duration).cast(pl.Int32).alias(Cols.seconds))
