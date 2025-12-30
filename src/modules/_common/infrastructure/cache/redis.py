from fastapi import Request
import redis.asyncio as redis
import json
from dataclasses import is_dataclass, asdict
from typing import Any, Optional
import redis.asyncio as redis
from modules._common.domain.interfaces.cache import ICache


async def get_redis_client(request: Request) -> redis.Redis:
    return request.app.state.redis

def _to_jsonable(value: Any) -> Any:
    """
    Convierte 'value' a algo JSON-serializable:
    - dict/list/str/int/float/bool/None -> ok
    - dataclass -> asdict
    - Pydantic v2 -> model_dump()
    - Pydantic v1 -> dict()
    - objetos con to_dict/dict -> dict()
    - fallback: str(value)
    """
    if value is None or isinstance(value, (str, int, float, bool)):
        return value

    if isinstance(value, (list, tuple)):
        return [_to_jsonable(v) for v in value]

    if isinstance(value, dict):
        return {str(k): _to_jsonable(v) for k, v in value.items()}

    if is_dataclass(value):
        return _to_jsonable(asdict(value))

    # Pydantic v2
    if hasattr(value, "model_dump") and callable(getattr(value, "model_dump")):
        return _to_jsonable(value.model_dump())

    # Pydantic v1 o similares
    if hasattr(value, "dict") and callable(getattr(value, "dict")):
        return _to_jsonable(value.dict())

    if hasattr(value, "to_dict") and callable(getattr(value, "to_dict")):
        return _to_jsonable(value.to_dict())

    # Fallback (no romper negocio)
    return str(value)


class RedisCache(ICache):
    """
    Cache Redis:
    - Guarda TODO como JSON (string)
    - Acepta primitives, list/tuple, dict y objects (dataclass/pydantic/clases)
    - Best-effort: si Redis falla, no rompe el flujo de negocio
    """

    def __init__(self, redis_client: redis.Redis):
        self._redis = redis_client

    async def get(self, key: str) -> Optional[Any]:
        try:
            data = await self._redis.get(key)
            if data is None:
                return None
            return json.loads(data)
        except Exception:
            return None

    async def set(self, key: str, value: Any, ttl_seconds: int) -> None:
        try:
            payload = json.dumps(_to_jsonable(value), ensure_ascii=False, separators=(",", ":"))
            await self._redis.setex(key, ttl_seconds, payload)
        except Exception:
            # cache es best-effort
            pass

    async def delete(self, key: str) -> None:
        try:
            await self._redis.delete(key)
        except Exception:
            pass

    async def exists(self, key: str) -> bool:
        try:
            return bool(await self._redis.exists(key))
        except Exception:
            return False
