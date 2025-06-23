from fastapi import Depends
from modules._common.infrastructure.db import get_db_saem3, get_db_masivos_sms, get_db_portabilidad
from modules.data_processing.infrastructure.db.db_context import DbContext
from sqlalchemy.orm import Session

def get_databases(
        saem3_db: Session =Depends(get_db_saem3),
        masivos_db: Session =Depends(get_db_masivos_sms),
        portabilidad_db: Session = Depends(get_db_portabilidad)
    ):
    return DbContext(saem3=saem3_db, masivos_sms=masivos_db, portabilidad_db=portabilidad_db)
