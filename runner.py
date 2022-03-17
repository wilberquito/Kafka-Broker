import datetime as dt
import json
from random import randrange
from multiprocessing import Process
import time
import requests
from requests import exceptions as rex
import pandas as pd
import logging
from sqlalchemy import create_engine, true
import constants as const

from yaml_parser.parser import Parser

logging.basicConfig(filename='runner.log', format='%(asctime)s %(levelname)s [%(filename)s:%(lineno)s]  %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
logger = logging.getLogger('runner.py')


def compute_execution_times(process_id: str, extras: dict) -> tuple[int, int, dict]:
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

def charge_from_database(url: str, sql: str) -> str:
    """ Consumes database and maps to json """
    engine = create_engine(url)
    df = pd.read_sql_query(sql, con=engine)
    return df.to_json(orient='records')

def map_json_to_consumer_format(json_data: str, localId: str) -> list:
    """ Maps json result from orient='records' configurations to list of json objets """
    elements = json.loads(json_data)
    result = []
    for elem in elements:
        column = dict(elem)
        store = {
            'localId': column.pop(localId),
            'object': column
        }
        result.append(store)
    return result

def send_to_consumer(user: str, passwd: str, api: str, message):
    """ Uses buffering api from IHUB project """
    headers = {'Content-Type': 'application/json', 'Accept':'application/json'}
    requests.post(api, auth=(user, passwd), data=json.dumps(message), headers=headers)


def map_and_send_to_consumer(json: str, localId: str, consumer: dict):
    """ Maps to the format that buffering api understands and then sends it """
    message: list = map_json_to_consumer_format(json, localId)
    kwards = consumer | { 'message': message }
    send_to_consumer(**kwards)
    
def charge_from_excel(path: str) -> str:
    df = pd.read_excel(path)
    return df.to_json(orient='records')

# def ftp_pipeline(extras: dict) -> None:
#     url, localId = extras.get(SETTING_PATH_TK), extras[SETTING_LOCAL_ID_TK]
#     api, user, passwd = extras[SETTING_API_TK], extras[SETTING_USER_TK], extras[SETTING_PASSWD_TK]
#     json = charge_from_excel(url)
#     message = mapping_response(json, localId)
#     send(api, user, passwd, message)        
#     return None

def database_pipeline(url, sql, localId, consumer) -> list:
    """ Consum data -> Map data -> Send data """
    json = charge_from_database(url, sql)
    return map_and_send_to_consumer(json, localId, consumer)    

def run(process_type: str, process_id: str, each: int, wait: int, extras: dict, dev: bool = False):
    """ Handles execution time and execution itself
    
        When one key is not found inside extras, process ends because a bad configuration
        or bad read parser configuration.
        
        Constraints: each, wait >= 0
    """
    if wait > 0:
        logger.info(f'Process - {process_id} - will make it first execution in - {wait} - seconds')
        time.sleep(wait)
        
    while True:
        logger.info(f'From process - {process_id} - about to load data, wish me luck')
        try:
            message = None
            start_time = dt.datetime.now()
            match process_type:
                case 'DATABASE':
                    message = database_pipeline(**extras)
                case 'FTP':
                    # message = ftp_pipeline(extras)
                    print('FTP EXECUTION PROCESSOS NOT IMPLEMENTED YET')
            execution_time = (dt.datetime.now() - start_time).total_seconds()*1000
            if dev and not message is None:
                with open('json_data.json', 'w') as out:
                    out.write(json.dumps(message, indent=4))
            
            logger.info(f'From process - {process_id} - It took {execution_time} ms')
        except rex.ConnectionError as err:
            logger.warning(f'From process - {process_id} - problem sending data. I\'ll try next time')
            logger.warning(err, exc_info=True)
        except Exception as err:
            logger.error(f'From process - {process_id} - unhandled exception')
            logger.error(err, exc_info=True)
        time.sleep(each if each >= 0 else 0)
        
def add_consumer_data(process_id: str, extras: dict, parser: Parser) -> dict:
    """ Matches consumer configuration in default with
        individual configurations. 
    """
    api = parser.api()
    if '{' + const.SETTING_CONNECTOR_NAME_TK + '}' in api:
        connector_name = extras.get(const.SETTING_CONNECTOR_NAME_TK)
        if connector_name is None:
            logger.error(f'api consumer should be replaced by there is no connector name defined in \'{process_id}\'')
            raise Exception(f'api consumer should be replaced by there is no connector name defined in \'{process_id}\'')
        else:
            api = api.replace('{' + const.SETTING_CONNECTOR_NAME_TK + '}', connector_name)
            extras.pop(const.SETTING_CONNECTOR_NAME_TK)
    user = parser.api_user()
    passwd = parser.api_password()
    consumer_data = {
        'user': user,
        'passwd': passwd,
        'api': api
    }
    result = dict(extras)
    result[const.SETTING_CONSUMER_TK] = consumer_data
    return result
    
     
if __name__ == '__main__':
    parser = Parser('settings.yaml')
    executions_list = parser.executions();
    dev = parser.dev_environment()
    
    all_processes = list()
    db_processes, ftp_processes = 0, 0
    
    for execution_type, execution in executions_list:
        process_id, settings = execution.get('process_id'), execution.get('settings')
        each, wait, settings = compute_execution_times(process_id, settings)
        settings = add_consumer_data(process_id, settings, parser)
        match execution_type:
            case const.DATABASE_TK:
                db_processes += 1
            case const.FTP_TK:
                ftp_processes += 1
            
        p = Process(target=run, args=(execution_type, process_id, each, wait, settings, dev,))
        all_processes.append(p)
        p.start()
  
    print(f"""
          Number of {const.DATABASE_TK} processes running {db_processes}
          Number of {const.FTP_TK} processes running {ftp_processes}
          """)
    # Avoids kill main thred waiting child processors die
    for p in all_processes:
        p.join()