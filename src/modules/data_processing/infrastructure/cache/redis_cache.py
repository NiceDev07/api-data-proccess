import json
from redis.asyncio import Redis
from modules.data_processing.domain.interfaces.cache_interface import ICache

class RedisCache(ICache):
    def __init__(self, client: Redis):
        self.client = client

    async def ping(self):
        return await self.client.ping()

    async def get(self, key: str):
        raw = await self.client.get(key)
        if not raw:
            return None  # Si no hay datos, devuelve None

        try:
            # Intentar decodificar como JSON
            return json.loads(raw)
        except json.JSONDecodeError:
            # Si no es JSON, devolver el texto plano
            return raw if isinstance(raw, bytes) else raw

    async def set(self, key: str, value, ttl: int = 3600):
       await self.client.setex(key, ttl, json.dumps(value))

    async def delete(self, key: str):
        await self.client.delete(key)
