from contextlib import asynccontextmanager
from fastapi import FastAPI
from dask.distributed import Client, LocalCluster
from config.settings import settings
from config.logger import get_logger

# logger = get_logger("uvicorn")

# @asynccontextmanager
# async def lifespan(app: FastAPI):
#     cluster = await LocalCluster(
#         n_workers=settings.DASK_N_WORKERS, #Indica el número de trabajadores (workers) que se arrancarán en el clúster. Es decir, cuántos procesos (o “nodos”) paralelos se usarán para ejecutar tareas.
#         threads_per_worker=settings.DASK_THREADS_PER_WORKER, #Indica el número de hilos (threads) que cada trabajador utilizará. Esto es útil para aprovechar los núcleos de CPU disponibles en cada máquina.
#         memory_limit=settings.DASK_MEMORY_LIMIT, #Especifica el límite de memoria que cada trabajador puede usar. Esto es importante para evitar que un trabajador consuma toda la memoria disponible y cause problemas en el sistema.
#         processes=True, #Indica si se deben usar procesos en lugar de hilos para cada trabajador. Esto puede ser útil para evitar problemas de bloqueo de memoria compartida y aprovechar mejor los recursos del sistema.
#         #asynchronous=True #Permite que el clúster se ejecute de manera asíncrona, lo que es útil para integrarse con aplicaciones asíncronas como FastAPI.
#     )
#     client = await Client(cluster, asynchronous=True)
#     app.state.dask_client = client
#     logger.info("🚀 Dask iniciado: %s", client.dashboard_link)

#     try:
#         yield
#     finally:
#         await client.close()
#         await cluster.close()
#         logger.info("🛑 Dask cerrado")

