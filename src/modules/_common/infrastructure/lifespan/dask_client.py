from contextlib import asynccontextmanager
from fastapi import FastAPI
from dask.distributed import Client, LocalCluster
from config.settings import settings
from config.logger import get_logger

logger = get_logger("uvicorn")

@asynccontextmanager
async def lifespan(app: FastAPI):
    cluster = LocalCluster(
        n_workers=settings.DASK_N_WORKERS,
        threads_per_worker=settings.DASK_THREADS_PER_WORKER,
        memory_limit=settings.DASK_MEMORY_LIMIT,
        processes=settings.DASK_PROCESSES,
    )
    client = Client(cluster)
    app.state.dask_client = client
    logger.info(f"🚀 Dask Client iniciado: {client.dashboard_link}")

    try:
        yield
    finally:
        client.close()
        logger.info("🛑 Dask Client cerrado")
