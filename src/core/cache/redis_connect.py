from redis import Redis

def get_redis_client():
    return Redis(host="localhost", port=6379, db=0, decode_responses=True)

# if __name__ == "__main__":
#     try:
#         client = get_redis_client()
#         response = client.ping()
#         if response:
#             print("✅ Conexión a Redis exitosa.")
#         else:
#             print("❌ Redis no respondió al ping.")
#     except Exception as e:
#         print(f"❌ Error al conectar con Redis: {e}")