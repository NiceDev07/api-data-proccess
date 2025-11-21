
from modules.process.domain.interfaces.file_reader import IFileReader

class XlsxReader(IFileReader):
    async def read(self, file):
        return {
            "status": "XLSX read",
            "success": True
        }