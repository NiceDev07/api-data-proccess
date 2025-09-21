from sqlalchemy.ext.asyncio import AsyncSession

class DbContext:
    def __init__(
        self,
        saem3: AsyncSession,
        masivos_sms: AsyncSession,
        portabilidad_db: AsyncSession
    ):
        self.saem3: AsyncSession = saem3
        self.masivos_sms: AsyncSession = masivos_sms
        self.portabilidad_db: AsyncSession = portabilidad_db
