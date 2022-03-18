from datetime import datetime
import logging
import constants as const

def _self_log() -> str:
    now = datetime.now()
    date_time = now.strftime("%m-%d-%Y-%H-%M")
    return const.APP_LOGGERS_DIRECTORY + const.APP_LOGGER_FILE_NAME + '-' + date_time + '.log'

logging.basicConfig(level=logging.NOTSET, filename=_self_log(), format='%(asctime)s %(levelname)s [%(filename)s:%(lineno)s]  %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
_logger: logging.Logger = logging.getLogger(const.APP_LOGGER_NAME)

def logger_app():
    return _logger