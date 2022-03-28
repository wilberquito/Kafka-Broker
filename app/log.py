from datetime import datetime
import logging
import constants as const
import os 


formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
def _self_log() -> str:
    if not os.path.exists(const.APP_LOGGERS_DIRECTORY):
        os.makedirs(const.APP_LOGGERS_DIRECTORY)
    now = datetime.now()
    date_time = now.strftime("%m-%d-%Y-%H-%M")
    return const.APP_LOGGERS_DIRECTORY + const.APP_LOGGER_FILE_NAME + '-' + date_time + '.log'

logging.basicConfig(level=logging.INFO, filename=_self_log(), format='%(asctime)s - %(levelname)s - %(module)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

# console handler
ch = logging.StreamHandler()
ch.setFormatter(formatter)
ch.setLevel(level=logging.DEBUG)

def setup_custom_logger():
    logger = logging.getLogger('root')
    logger.addHandler(ch)
    return logger

# _logger: logging.Logger = logging.getLogger(const.APP_LOGGER_NAME)


