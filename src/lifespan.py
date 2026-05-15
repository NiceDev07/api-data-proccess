import re
from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
import redis.asyncio as redis

from config.settings import settings  # type: ignore
from logging_config import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)

# ── args compartidos ──────────────────────────────────────────────────────────

_ASYNC_ENGINE_ARGS = dict(
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
    pool_recycle=1800,
)

# Pools sync más pequeños: solo los usan los confirm (LOAD DATA / batch INSERT)
_SYNC_ENGINE_ARGS = dict(
    pool_size=5,
    max_overflow=5,
    pool_pre_ping=True,
    pool_recycle=1800,
)


def _sync_dsn(dsn: str) -> str:
    """Convierte DSN asyncmy → pymysql para el engine síncrono."""
    return re.sub(r"^mysql\+\w+://", "mysql+pymysql://", dsn)


# ── lifespan ──────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ───────────────────────────────────────────────────────────────
    logger.info("Inicializando pools async de MySQL...")
    engines = {
        "saem3": create_async_engine(settings.DB_SAEM3, **_ASYNC_ENGINE_ARGS),
        "portabilidad": create_async_engine(settings.DB_PORTABILIDAD, **_ASYNC_ENGINE_ARGS),
        "masivos_sms": create_async_engine(
            settings.DB_MASIVOS_SMS,
            connect_args={"charset": "utf8mb4"},
            **_ASYNC_ENGINE_ARGS,
        ),
        "telefonos_campanas": create_async_engine(
            settings.DB_TELEFONOS_CAMPANAS,
            connect_args={"charset": "utf8mb4"},
            **_ASYNC_ENGINE_ARGS,
        ),
        "email": create_async_engine(
            settings.DB_EMAIL,
            connect_args={"charset": "utf8mb4"},
            **_ASYNC_ENGINE_ARGS,
        ),
    }

    # Engine síncrono solo para campanas (pymysql + local_infile).
    logger.info("Inicializando pools sync de MySQL...")
    sync_engines = {
        "telefonos_campanas": create_engine(
            _sync_dsn(settings.DB_TELEFONOS_CAMPANAS),
            connect_args={"local_infile": True, "charset": "utf8mb4"},
            **_SYNC_ENGINE_ARGS,
        ),
    }

    logger.info("Inicializando pool de Redis...")
    redis_client = redis.from_url(
        settings.REDIS_URL,
        encoding="utf-8",
        decode_responses=True,
        max_connections=50,
        db=0,
    )

    # Health check rápido — falla temprano si algo no conecta
    try:
        await redis_client.ping()
        logger.info("Redis OK (PING)")
    except Exception:
        logger.exception("Fallo en healthcheck de dependencias")
        try:
            await redis_client.close()
            await redis_client.wait_closed()
        except Exception:
            logger.exception("Error cerrando Redis tras fallo en startup")
        for name, engine in engines.items():
            try:
                await engine.dispose()
            except Exception:
                logger.exception("Error cerrando async engine %s tras fallo en startup", name)
        for name, engine in sync_engines.items():
            try:
                engine.dispose()
            except Exception:
                logger.exception("Error cerrando sync engine %s tras fallo en startup", name)
        raise

    from modules.process.infrastructure.deps import build_process_shared_deps
    process_deps = build_process_shared_deps(
        redis_client,
        max_records_elevated=settings.MAX_CAMPAIGN_RECORDS,
    )

    session_factories = {
        name: async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
        for name, engine in engines.items()
    }

    app.state.engines           = engines
    app.state.sync_engines      = sync_engines
    app.state.session_factories = session_factories
    app.state.redis             = redis_client
    app.state.process_deps      = process_deps

    try:
        yield  # ── app corriendo ─────────────────────────────────────────────
    finally:
        # ── Shutdown ──────────────────────────────────────────────────────────
        logger.info("Cerrando async engines...")
        for name, engine in engines.items():
            try:
                await engine.dispose()
                logger.info("Async engine %s cerrado", name)
            except Exception:
                logger.exception("Error cerrando async engine %s", name)

        logger.info("Cerrando sync engines...")
        for name, engine in sync_engines.items():
            try:
                engine.dispose()
                logger.info("Sync engine %s cerrado", name)
            except Exception:
                logger.exception("Error cerrando sync engine %s", name)

        logger.info("Cerrando Redis...")
        try:
            await redis_client.close()
            logger.info("Redis cerrado")
        except Exception:
            logger.exception("Error cerrando Redis")
