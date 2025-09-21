from contextlib import asynccontextmanager
from fastapi import FastAPI
from config.logger import get_logger

logger = get_logger("uvicorn")

@asynccontextmanager
async def lifespan(app: FastAPI):
    yield

