import asyncio

from modules.process.infrastructure.audio._ffprobe import _EXTRA_SECONDS, probe_duration


class FfprobeAudioDurationProvider:
    """Obtiene la duración de un archivo de audio **local** usando ffprobe."""

    def __init__(self, extra_seconds: int = _EXTRA_SECONDS):
        self._extra = extra_seconds

    async def get_duration(self, path: str) -> float:
        return await asyncio.to_thread(probe_duration, path, self._extra)
