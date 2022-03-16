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
from constants import API_TK, CONNECTOR_NAME_TK, EACH_TK, LOCAL_ID_TK, PASSWORD_TK, PATH_TK, SQL_TK, URL_TK, USER_TK, WAIT_TK

from yaml_parser.parser import Parser

logging.basicConfig(filename='runner.log', format='%(asctime)s %(levelname)s [%(filename)s:%(lineno)s]  %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
logger = logging.getLogger('runner.py')


def compute_execution_times(process_id: str, extras: dict) -> tuple[int, int, dict]:
    ''' Returns configured time for `wait` & `each`. Mutates dictionary deleting them from it
    
    If default configuration is not found for each & wait, function will generate a random
    number to continue the execution. 
    
    Time configured is represented in seconds.
    '''
    each_bc = extras.get(EACH_TK) is None or extras.get(EACH_TK) < 0
    wait_bc = extras.get(WAIT_TK) is None or extras.get(WAIT_TK) < 0
    
    if each_bc:
        logger.warning(f'No configuration found or bad configuration for - {process_id} - in \'each\' definition. A random number will be generated')
        
    if wait_bc:
        logger.warning(f'No configuration found or bad configuration for - {process_id} - in \'each\' definition. A random number will be generated')
    
    each = randrange(60*2, 60*30) if each_bc else extras.get(EACH_TK)
    wait = randrange(0, 60*2) if wait_bc else extras.get(WAIT_TK)
    
    if EACH_TK in extras:
        extras.pop(EACH_TK)
    if WAIT_TK in extras:
        extras.pop(WAIT_TK)
        
    return (each, wait, extras)

def charge_from_database(url: str, sql: str) -> str:
    engine = create_engine(url)
    df = pd.read_sql_query(sql, con=engine)
    return df.to_json(orient='records')

def mapping_response(response: str, local_id: str) -> list:
    elements = json.loads(response)
    result = []
    for elem in elements:
        column = dict(elem)
        store = {
            'localId': column.pop(local_id) ,
            'object': column
        }
        result.append(store)
    return result

def send(api: str, user: str, password: str, message: str):
    headers = {'Content-Type': 'application/json', 'Accept':'application/json'}
    requests.post(api, auth=(user, password), data=json.dumps(message), headers=headers)

def database_pipeline(extras: dict) -> list:
    ''' Consum data -> Map data -> Send data '''
    api, user, password = extras[API_TK], extras[USER_TK], extras[PASSWORD_TK]
    url, sql, local_id = extras[URL_TK], extras[SQL_TK], extras[LOCAL_ID_TK]
    json_response = charge_from_database(url, sql)
    message = mapping_response(json_response, local_id)
    send(api, user, password, message)
    
    return message    

def charge_from_excel(path: str) -> str:
    df = pd.read_excel(path)
    return df.to_json(orient='records')

def excel_pipeline(extras: dict) -> None:
    url, localId = extras.get(PATH_TK), extras[LOCAL_ID_TK]
    api, user, password = extras[API_TK], extras[USER_TK], extras[PASSWORD_TK]
    json = charge_from_excel(url)
    message = mapping_response(json, localId)
    send(api, user, password, message)        
    return None

def run(process_type: str, process_id: str, each: int, wait: int, extras: dict, dev: bool = False):
    ''' Handles execution time and execution itself
    
        When one key is not found inside extras, process ends because a bad configuration
        or bad read parser configuration.
        
        Constraints: each, wait >= 0
    '''
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
                    message = database_pipeline(extras)
                case 'EXCEL':
                    message = excel_pipeline(extras)
                    print('EXCEL EXECUTION PROCESSOS NOT IMPLEMENTED YET')
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
        
def match_data(process_id: str, extras: dict, parser: Parser) -> dict:
    ''' Matches consumer configuration in default with
        individual configurations. 
        
        This function return a joined dictionary 
        between `extras` & `{ user, password, api }`
    '''
    api = parser.api()
    replace =  '{' + CONNECTOR_NAME_TK + '}' in api
    
    if replace:
        connector_name = extras.get(CONNECTOR_NAME_TK)
        if connector_name is None:
            logger.error(f'api consumer should be replaced by there is no connector name defined in \'{process_id}\'')
            raise Exception(f'api consumer should be replaced by there is no connector name defined in \'{process_id}\'')
        else:
            api = api.replace('{' + CONNECTOR_NAME_TK + '}', connector_name)
    user = parser.api_user()
    password = parser.api_password()
    consumer_data = {
        'user': user,
        'password': password,
        'api': api
    }
    return dict(extras, **consumer_data)
     
if __name__ == '__main__':
    parser = Parser('settings.yaml')
    executions_list = parser.executions();
    dev = parser.dev_environment()
    
    all_processes = list()
    db_processes, excel_process = 0, 0
    
    for execution_type, execution in executions_list:
        process_id, settings = execution.get('process_id'), execution.get('settings')
        each, wait, settings = compute_execution_times(process_id, settings)
        settings = match_data(process_id, settings, parser)
        match execution_type:
            case 'DATABASE':
                db_processes += 1
            case 'EXCEL':
                excel_process += 1
            
        p = Process(target=run, args=(execution_type, process_id, each, wait, settings, dev,))
        all_processes.append(p)
        p.start()
  
    print(f"""
          Number of database processes running {db_processes}
          Number of excel processes running {excel_process}
          """)
    # Avoids kill main thred waiting child processors die
    for p in all_processes:
        p.join()