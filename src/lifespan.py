from fastapi import FastAPI, Depends
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
import redis.asyncio as redis
from config.settings import settings
from config.logger import get_logger

logger = get_logger("uvicorn")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Inicializar engines de MySQL (cada uno es un pool de conexiones)
    engines = {
        "saem3": create_async_engine(settings.DB_SAEM3, pool_size=10, max_overflow=20, pool_pre_ping=True, pool_recycle=1800),
        "portabilidad": create_async_engine(settings.DB_PORTABILIDAD, pool_size=10, max_overflow=20, pool_pre_ping=True, pool_recycle=1800),
        "masivos_sms": create_async_engine(settings.DB_MASIVOS_SMS, pool_size=10, max_overflow=20, pool_pre_ping=True, pool_recycle=1800),
    }
    
    app.state.engines = engines

    # 2. Redis pool
    redis_client = redis.from_url(
        "redis://localhost:6379/0",
        encoding="utf-8",
        decode_responses=True,
        max_connections=50,
    )
    # Verificamos conexión inicial
    await redis_client.ping()
    app.state.redis = redis_client
    yield

