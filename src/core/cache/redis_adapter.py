import json
from redis import Redis
from .interfaces.cache_interface import CacheInterface

class RedisCache(CacheInterface):
    def __init__(self, client: Redis):
        self.client = client

    def ping(self):
        return self.client.ping()

    def get(self, key: str):
        raw = self.client.get(key)
        if not raw:
            return None  # Si no hay datos, devuelve None

        try:
            # Intentar decodificar como JSON
            return json.loads(raw)
        except json.JSONDecodeError:
            # Si no es JSON, devolver el texto plano
            return raw if isinstance(raw, bytes) else raw

    def set(self, key: str, value, ttl: int = 3600):
        self.client.setex(key, ttl, json.dumps(value))

    def delete(self, key: str):
        self.client.delete(key)
