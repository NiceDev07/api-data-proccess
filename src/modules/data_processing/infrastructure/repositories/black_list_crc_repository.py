from modules.data_processing.domain.interfaces.black_list_crc_repository import IBlackListCRCRepository
from ..models.black_list import BlackList
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

class BlackListCRCRepository(IBlackListCRCRepository):
    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_black_list_crc(self, country_id: int = 81) -> list[int]:
        stmt = (
            select(BlackList.source_addr)
            .where(BlackList.status == 'A', BlackList.country_id == country_id)
        )
        result = await self.db.execute(stmt)      # ← await
        return result.scalars().all()    