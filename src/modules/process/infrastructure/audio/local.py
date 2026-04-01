import asyncio

from modules.process.infrastructure.audio._ffprobe import _EXTRA_SECONDS, probe_duration


class FfprobeAudioDurationProvider:
    """Obtiene la duración de un archivo de audio **local** usando ffprobe."""

    def __init__(self, extra_seconds: int = _EXTRA_SECONDS):
        self._extra = extra_seconds

    async def get_duration(self, path: str) -> float:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, probe_duration, path, self._extra)
