
from random import randrange

from yaml_parser.parser import Parser
import constants as const
from runner import run

from loggerapp import logger_app

logger = logger_app()

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

def compute_execution_times(process_id: str, extras: dict) -> tuple[int, int, dict]:
    """ Returns configured time for `wait` & `each`. Mutates dictionary deleting them from it
    
    If default configuration is not found for each & wait, function will generate a random
    number to continue the execution. 
    
    Time configured is represented in seconds.
    """
    repeat_bc = extras.get(const.SETTING_REPEAT_TK) is None or extras.get(const.SETTING_REPEAT_TK) < 0
    wait_bc = extras.get(const.SETTING_WAIT_TK) is None or extras.get(const.SETTING_WAIT_TK) < 0
    
    if repeat_bc:
        logger.warning(f'No configuration found or bad configuration for - {process_id} - in \'repeat\' definition. A random number will be generated')
        
    if wait_bc:
        logger.warning(f'No configuration found or bad configuration for - {process_id} - in \'wait\' definition. A random number will be generated')
    
    repeat = randrange(60*2, 60*30) if repeat_bc else extras.get(const.SETTING_REPEAT_TK)
    wait = randrange(0, 60*2) if wait_bc else extras.get(const.SETTING_WAIT_TK)
    
    if const.SETTING_REPEAT_TK in extras:
        extras.pop(const.SETTING_REPEAT_TK)
    if const.SETTING_WAIT_TK in extras:
        extras.pop(const.SETTING_WAIT_TK)
        
    return (repeat, wait, extras)

def byebye():
    logger.info('User requested shutdown...')
    logger.info('Ready to exit, bye bye...')
    
if __name__ == '__main__':
    try:
        parser = Parser(const.SETTINGS_FILE_NAME)
        consumer = parser.defaults()['consumer']
        executions = parser.executions();
        run_executions_list = []
        for process_type, process in executions:
            process_id, context = process.get(const.PROCESS_ID), process.get(const.PROCESS_CONTEXT)
            context = match_consumer_data(context, consumer)
            repeat, wait, context = compute_execution_times(process_id, context)
            run_executions_list.append({
                const.PROCESS_TYPE: process_type,
                const.PROCESS_ID: process_id,
                const.PROCESS_REPEAT: repeat,
                const.PROCESS_WAIT: wait,
                const.PROCESS_CONTEXT: context
            })
        run(run_executions_list)
    except KeyboardInterrupt:
        byebye()        
    
    