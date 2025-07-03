from modules.data_processing.domain.interfaces.black_list_crc_repository import IBlackListCRCRepository
from sqlalchemy.orm import Session
from ..models.black_list import BlackList
from sqlalchemy import select

class BlackListCRCRepository(IBlackListCRCRepository):
    def __init__(self, db: Session):
        self.db = db

    def get_black_list_crc(self, country_id: int = 81) -> list[int]:
        try:
            stmt = select(BlackList.source_addr).where(
                BlackList.status == 'A',
                BlackList.country_id == country_id
            )
            result = self.db.execute(stmt)
            return result.scalars().all()
        except Exception as e:
            raise e