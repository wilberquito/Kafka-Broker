from datetime import datetime
import logging
import os 
import settings_parser as parser

formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
def _self_log() -> str:
    if not os.path.exists(parser.APP_LOGGERS_DIRECTORY):
        os.makedirs(parser.APP_LOGGERS_DIRECTORY)
    now = datetime.now()
    date_time = now.strftime("%m-%d-%Y-%H-%M")
    return parser.APP_LOGGERS_DIRECTORY + parser.APP_LOGGER_FILE_NAME + '-' + date_time + '.log'

logging.basicConfig(level=logging.INFO, filename=_self_log(), format='%(asctime)s - %(levelname)s - %(module)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

# console handler
ch = logging.StreamHandler()
ch.setFormatter(formatter)
ch.setLevel(level=logging.DEBUG)

def setup_custom_logger():
    logger = logging.getLogger('root')
    logger.addHandler(ch)
    return logger

