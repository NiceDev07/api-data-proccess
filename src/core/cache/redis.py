from redis import Redis

def get_redis_client():
    return Redis(host="localhost", port=6379, db=0, decode_responses=True)
