import logging
from dataclasses import dataclass
from logging.handlers import SysLogHandler
from pathlib import Path
from typing import ClassVar

logfile_path = Path(__file__).parent / 'logs' / 'campaign_finance.log'

@dataclass
class Logger:
    project_name: str
    module_name: str = "campaignfinance"
    formatter = logging.Formatter('%(asctime)s  %(name)s  %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    silent_error_format = logging.Formatter('%(asctime)s  %(name)s  SILENT %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    __PAPERTRAIL_HOST = "logs4.papertrailapp.com"
    __PAPERTRAIL_PORT = 33096

    @property
    def logger_name(self):
        return Path(self.module_name).stem

    def __post_init__(self):
        self.logger = logging.Logger(f"{Logger.module_name}@{self.logger_name}")
        self.logger.setLevel(level=logging.DEBUG)

        # PaperTrail Logging Settings
        remote_handler = SysLogHandler(address=(self.__PAPERTRAIL_HOST, self.__PAPERTRAIL_PORT))
        remote_handler.setFormatter(self.formatter)

        # Local Timed Logging Settings
        timed_logfile = logging.handlers.TimedRotatingFileHandler(logfile_path, when='D', interval=7)
        timed_logfile.setFormatter(self.formatter)

        # Console Logging Settings
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(self.formatter)

        # Console-Silent Logging Settings
        silent_error_handler = remote_handler
        silent_error_handler.setFormatter(self.silent_error_format)

        silent_error_local_handler = timed_logfile
        self.error_logger = logging.Logger(f"{Logger.module_name}@{self.logger_name}")
        self.error_logger.setLevel(level=logging.ERROR)
        self.error_logger.addHandler(silent_error_handler)
        self.error_logger.addHandler(silent_error_local_handler)

        self.logger.addHandler(remote_handler)
        self.logger.addHandler(timed_logfile)
        self.logger.addHandler(console_handler)

    def info(self, message):
        self.logger.info(message)

    def debug(self, message):
        self.logger.debug(message)

    def warning(self, message):
        self.logger.warning(message)

    def error(self, message):
        self.logger.error(message)

    def critical(self, message):
        self.logger.critical(message)

    def exception(self, message):
        self.logger.exception(message)

    def silent_error(self, message):
        self.error_logger.error(message)
