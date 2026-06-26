from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
# from modules.data_processing.infrastructure.routes.preload_camp import router as preload_router
from modules.process.infrastructure.routes.filter import router as filter_router
from modules.process.infrastructure.routes.process import router as process_router
from modules.process.infrastructure.routes.preview import router as preview_router
from modules.process.infrastructure.routes.email_test import router as email_test_router
from modules.process.infrastructure.routes.docs import (
    APP_TITLE, APP_VERSION, APP_DESCRIPTION, OPENAPI_TAGS,
)
from modules.process.infrastructure.errors import build_error_detail
from config.settings import settings
from lifespan import lifespan

def create_app() -> FastAPI:
    app = FastAPI(
        title=APP_TITLE,
        description=APP_DESCRIPTION,
        version=APP_VERSION,
        lifespan=lifespan,
        openapi_tags=OPENAPI_TAGS,
    )
    # CORS abierto — la autenticación está delegada al API Gateway externo.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
        error = exc.errors()[0]
        # Pydantic prefija los validators custom con "Value error, " — lo quitamos para
        # que build_error_detail extraiga correctamente el CODE del mensaje.
        msg = error["msg"].removeprefix("Value error, ")
        status_code = 400 if (error.get("loc") and error["loc"][0] == "path") else 422
        return JSONResponse(status_code=status_code, content={"detail": build_error_detail(msg)})

    # app.include_router(preload_router, prefix=settings.prefix_app, tags=["Data Processing Service"])
    app.include_router(process_router, prefix=settings.prefix_app)
    app.include_router(preview_router, prefix=settings.prefix_app)
    app.include_router(email_test_router, prefix=settings.prefix_app)
    app.include_router(filter_router, prefix=settings.prefix_app)
    return app

app = create_app()
