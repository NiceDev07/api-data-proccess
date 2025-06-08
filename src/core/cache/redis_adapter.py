import json
from redis import Redis
from .interfaces.cache_interface import CacheInterface

class RedisCache(CacheInterface):
    def __init__(self, client: Redis):
        self.client = client

    def get(self, key: str):
        raw = self.client.get(key)
        return json.loads(raw) if raw else None

    def set(self, key: str, value, ttl: int = 3600):
        self.client.setex(key, ttl, json.dumps(value))

    def delete(self, key: str):
        self.client.delete(key)
