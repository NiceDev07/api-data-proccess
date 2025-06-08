from .logger_config import get_logger

class LoggerAdapter:
    def __init__(self, logger_name=None):
        self.logger = get_logger(logger_name)

    def info(self, msg: str):
        self.logger.info(msg)

    def error(self, msg: str):
        self.logger.error(msg)

    def exception(self, msg: str):
        self.logger.exception(msg)

    def warning(self, msg: str):
        self.logger.warning(msg)
