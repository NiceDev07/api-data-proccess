import asyncio
from pathlib import Path
import polars as pl
from modules.process.domain.interfaces.storage import IStorage
from config.settings import settings


class LocalStorage(IStorage):
    def __init__(self, output_dir: str = settings.OUTPUT_DIR):
        self.output_dir = Path(output_dir)

    async def save(self, df: pl.DataFrame, filename: str) -> str:
        path = self.output_dir / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(df.write_parquet, path, compression="zstd")
        return str(path)
