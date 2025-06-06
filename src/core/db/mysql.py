from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base

# Base común si compartes modelos (opcional)
Base = declarative_base()

# --- MySQL DB (e.g. tarifas) ---
MYSQL_DATABASE_URL = "mysql+pymysql://jguzman:Jguzman/0922@192.168.0.33:3306/saem3"
mysql_engine = create_engine(MYSQL_DATABASE_URL)
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
