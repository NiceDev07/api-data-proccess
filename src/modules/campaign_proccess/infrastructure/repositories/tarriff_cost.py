# tariff_repository.py

from sqlalchemy.orm import Session
from redis import Redis
import json

class TariffRepository:
    def __init__(self, db: Session, redis_client: Redis):
        self.db = db
        self.redis = redis_client

    def get_tariff(self, country_id: int, tariff_id: int, service: str) -> dict:
        cache_key = f"tariff:{country_id}:{tariff_id}:{service}:cost"
        
        # 1. Intentar desde Redis
        cached_tariff = self.redis.get(cache_key)
        if cached_tariff:
            return json.loads(cached_tariff)

        # 2. Consultar base de datos MySQL
        result = self.db.execute(
            "SELECT code, price FROM tariffs WHERE country_code = %s", (country_id,)
        ).fetchone()
        
        if not result:
            raise ValueError(f"Tarifa no encontrada para país {country_id}")

        # 3. Guardar en Redis por 1h y retornar
        tariff_data = {"code": result.code, "price": float(result.price)}
        self.redis.setex(cache_key, 3600, json.dumps(tariff_data))
        return tariff_data
