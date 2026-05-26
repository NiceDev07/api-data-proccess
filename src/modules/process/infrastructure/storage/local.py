import asyncio
from pathlib import Path
import polars as pl
from modules.process.domain.interfaces.storage import IStorage
from config.settings import settings


class LocalStorage(IStorage):
    def __init__(self, base_dir: str = settings.repository_files_dir):
        self.base_dir = Path(base_dir)

    async def save(self, df: pl.DataFrame, filename: str) -> str:
        path = self.base_dir / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(df.write_parquet, path, compression="zstd")
        return str(path)
