# src/modules/data_processing/infrastructure/database/db_context.py
from sqlalchemy.orm import Session

class DbContext:
    def __init__(self, saem3: Session, masivos_sms: Session):
        self.saem3: Session = saem3
        self.masivos_sms: Session = masivos_sms
