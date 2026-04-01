import asyncio
import os
import tempfile

import httpx

from modules.process.infrastructure.audio._ffprobe import _EXTRA_SECONDS, probe_duration


class RemoteAudioDurationProvider:
    """Descarga el archivo de audio desde una URL y obtiene su duración con ffprobe."""

    def __init__(self, extra_seconds: int = _EXTRA_SECONDS, timeout: float = 30.0):
        self._extra = extra_seconds
        self._timeout = timeout

    async def get_duration(self, url: str) -> float:
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.get(url)
            response.raise_for_status()

        suffix = os.path.splitext(url.split("?")[0])[-1] or ".audio"
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
            tmp.write(response.content)
            tmp_path = tmp.name

        try:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, probe_duration, tmp_path, self._extra)
        finally:
            os.unlink(tmp_path)
