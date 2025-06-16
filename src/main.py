from fastapi import FastAPI
from modules.campaign_proccess.infrastructure.routes.preload_camp import router as preload_router
from dask_client import lifespan  # Assuming you have a lifespan_dask.py file with the lifespan context manager

app = FastAPI(lifespan=lifespan)
prefix = '/v2'
dask_manager = None  # Assuming you have a Dask manager defined elsewhere

app.include_router(preload_router, prefix=prefix, tags=["preload_campaigns"])


#Config Dask con fastapi
@app.on_event("startup")
async def startup_event():
    dask_manager.start()
    print("🚀 Dask Client started:", dask_manager.get_client().dashboard_link)

@app.on_event("shutdown")
async def shutdown_event():
    dask_manager.close()
    print("🛑 Dask Client closed.")