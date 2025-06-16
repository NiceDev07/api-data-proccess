# lifespan_dask.py
from contextlib import asynccontextmanager
from fastapi import FastAPI
from dask.distributed import Client, LocalCluster

@asynccontextmanager
async def lifespan(app: FastAPI):
    cluster = LocalCluster(
        n_workers=6,             # Ajusta según núcleos de tu máquina
        threads_per_worker=4,    # Hilos por proceso
        memory_limit="2GB",      # Por worker
        processes=True
    )
    client = Client(cluster)
    app.state.dask_client = client
    print("🚀 Dask Client iniciado:", client.dashboard_link)

    try:
        yield
    finally:
        client.close()
        print("🛑 Dask Client cerrado")
