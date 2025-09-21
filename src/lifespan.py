from contextlib import asynccontextmanager
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import create_async_engine
import redis.asyncio as redis
import logging
from config.settings import settings  # type: ignore

logger = logging.getLogger("uvicorn")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- Startup ---
    logger.info("Inicializando pools de MySQL...")
    engines = {
        "saem3": create_async_engine(
            settings.DB_SAEM3,
            pool_size=10, max_overflow=20,
            pool_pre_ping=True, pool_recycle=1800,
        ),
        "portabilidad": create_async_engine(
            settings.DB_PORTABILIDAD,
            pool_size=10, max_overflow=20,
            pool_pre_ping=True, pool_recycle=1800,
        ),
        "masivos_sms": create_async_engine(
            settings.DB_MASIVOS_SMS,
            pool_size=10, max_overflow=20,
            pool_pre_ping=True, pool_recycle=1800,
        ),
    }

    logger.info("Inicializando pool de Redis...")
    redis_client = redis.from_url(
        settings.REDIS_URL,              # ej: "redis://localhost:6379/0"
        encoding="utf-8",
        decode_responses=True,
        max_connections=50,
    )

    # Health checks (fallar temprano si algo no conecta)
    try:
        await redis_client.ping()
        logger.info("Redis OK (PING)")

        # opcional: probar un SELECT 1 en una BD
        # async with engines["saem3"].connect() as conn:
        #     await conn.execute(text("SELECT 1"))
        #     logger.info("MySQL saem3 OK (SELECT 1)")
    except Exception:
        # Si falla el healthcheck, limpiar lo ya creado y relanzar
        logger.exception("Fallo en healthcheck de dependencias")
        # Redis
        try:
            await redis_client.close()
            await redis_client.wait_closed()
        except Exception:
            logger.exception("Error cerrando Redis tras fallo en startup")
        # Engines
        for name, engine in engines.items():
            try:
                await engine.dispose()
            except Exception:
                logger.exception("Error cerrando engine %s tras fallo en startup", name)
        raise

    # Exponer en app.state SOLO si todo OK
    app.state.engines = engines
    app.state.redis = redis_client

    try:
        yield  # --- aquí corre la app ---
    finally:
        # --- Shutdown ---
        logger.info("Cerrando pools de MySQL...")
        for name, engine in engines.items():
            try:
                await engine.dispose()
                logger.info("Engine %s cerrado", name)
            except Exception:
                logger.exception("Error cerrando engine %s", name)

        logger.info("Cerrando Redis...")
        try:
            await redis_client.close()
            logger.info("Redis cerrado")
        except Exception:
            logger.exception("Error cerrando Redis")
