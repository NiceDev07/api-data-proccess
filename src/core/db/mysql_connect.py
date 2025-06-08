from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base
from config import settings

# Base común si compartes modelos (opcional)
Base = declarative_base()

mysql_engine = create_engine(settings.DB_SAEM3)
MySQLSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=mysql_engine)

def get_mysql_db():
    db = MySQLSessionLocal()
    try:
        yield db
    finally:
        db.close()

# TEST FAST CONNECTION
# if __name__ == "__main__":
#     try:
#         with mysql_engine.connect() as connection:
#             result = connection.execute(text("SELECT 1"))
#             print("✅ Conexión exitosa:", result.scalar())  # Debería imprimir: 1
#     except Exception as e:
#         print("❌ Error al conectar a la base de datos:", e)
