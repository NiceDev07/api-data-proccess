from modules.campaign_proccess.domain.interfaces.black_list_crc_repository import IBlackListCRCRepository
from sqlalchemy.orm import Session
from ..models.black_list import BlackList

class BlackListCRCRepository(IBlackListCRCRepository):
    def __init__(self, db: Session):
        self.db = db

    def get_black_list_crc(self, country_id: int = 81) -> list[int]:
        try:
            result = (
                self.db.query(BlackList.source_addr)
                .filter(
                    BlackList.status == 'A',
                    BlackList.country_id == country_id
                )
                .all()
            )
            return [result[0] for result in result] if result else []
        except Exception as e:
            self.db.rollback()
            raise e