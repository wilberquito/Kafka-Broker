
import logging
from datetime import datetime
from random import randrange

from yaml_parser.parser import Parser
import constants as const

def _self_log() -> str:
    now = datetime.now()
    date_time = now.strftime("%m-%d-%Y-%H-%M-%S")
    return const.APP_LOGGERS_DIRECTORY + const.APP_LOGGER_FILE_NAME + '-' + date_time + '.log'

logging.basicConfig(level=logging.NOTSET, filename=_self_log(), format='%(asctime)s %(levelname)s [%(filename)s:%(lineno)s]  %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
logger = logging.getLogger(const.APP_LOGGER_NAME)

def match_consumer_data(conf: dict, consumer: dict) -> dict:
    """ Matches consumer configuration in default with
        individual configurations. 
    """
    user, passwd, api = consumer[const.SETTING_USER_TK], consumer[const.SETTING_PASSWD_TK], consumer[const.SETTING_API_TK]
    
    if '{' + const.SETTING_CONNECTOR_NAME_TK + '}' in api:
        connector_name = conf[const.SETTING_CONNECTOR_NAME_TK]
        api = api.replace('{' + const.SETTING_CONNECTOR_NAME_TK + '}', connector_name)
        conf.pop(const.SETTING_CONNECTOR_NAME_TK)
    else:
        if const.SETTING_CONNECTOR_NAME_TK in conf:
            conf.pop(const.SETTING_CONNECTOR_NAME_TK)

    consumer_data = {
        'user': user,
        'passwd': passwd,
        'api': api
    }

    result = dict(conf)
    result[const.SETTING_CONSUMER_TK] = consumer_data

    return result


def _compute_execution_times(process_id: str, extras: dict) -> tuple[int, int, dict]:
    """ Returns configured time for `wait` & `each`. Mutates dictionary deleting them from it
    
    If default configuration is not found for each & wait, function will generate a random
    number to continue the execution. 
    
    Time configured is represented in seconds.
    """
    each_bc = extras.get(const.SETTING_REPEAT_TK) is None or extras.get(const.SETTING_REPEAT_TK) < 0
    wait_bc = extras.get(const.SETTING_WAIT_TK) is None or extras.get(const.SETTING_WAIT_TK) < 0
    
    if each_bc:
        logger.warning(f'No configuration found or bad configuration for - {process_id} - in \'each\' definition. A random number will be generated')
        
    if wait_bc:
        logger.warning(f'No configuration found or bad configuration for - {process_id} - in \'each\' definition. A random number will be generated')
    
    each = randrange(60*2, 60*30) if each_bc else extras.get(const.SETTING_REPEAT_TK)
    wait = randrange(0, 60*2) if wait_bc else extras.get(const.SETTING_WAIT_TK)
    
    if const.SETTING_REPEAT_TK in extras:
        extras.pop(const.SETTING_REPEAT_TK)
    if const.SETTING_WAIT_TK in extras:
        extras.pop(const.SETTING_WAIT_TK)
        
    return (each, wait, extras)


if __name__ == '__main__':
    parser = Parser(const.SETTINGS_FILE_NAME)
    consumer = parser.defaults()['consumer']
    executions = parser.executions();
    
    runner_executions = []
    
    for ex_type, ex in executions:
        process_id, conf = ex.get(const.PROCESS_ID), ex.get(const.PROCESS_SETTINGS)
        conf = match_consumer_data(conf, consumer)
        
        print(conf)
        
    
    