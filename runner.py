import datetime as dt
import json
from random import randrange
from multiprocessing import Process
import time
import requests
import pandas as pd
import logging
from sqlalchemy import create_engine
from constants import API_TK, CONNECTOR_NAME_TK, EACH_TK, LOCAL_ID_TK, PASSWORD_TK, SQL_TK, URL_TK, USER_TK, WAIT_TK

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

def charge(url: str, sql: str) -> str:
    engine = create_engine(url)
    df = pd.read_sql_query(sql, con=engine)
    return df.to_json(orient='records')

def mapping_response(response: str, localId: str) -> list:
    items = json.loads(response)
    result = []
    for item in items:
        map = dict(item)
        localId = map.pop('gid')
        store = {
            'localId': localId,
            'object': map
        }
        result.append(store)
    return result

def send(api, user, password, message):
    headers = {'Content-Type': 'application/json', 'Accept':'application/json'}
    requests.post(api, auth=(user, password), data=json.dumps(message), headers=headers)

def pipeline(extras: dict) -> list:
    ''' Consum data -> Map data -> Send data '''
    api, user, password = extras[API_TK], extras[USER_TK], extras[PASSWORD_TK]
    url, sql, local_id = extras[URL_TK], extras[SQL_TK], extras[LOCAL_ID_TK]
    json_response = charge(url, sql)
    message = mapping_response(json_response, local_id)
    send(api, user, password, message)
    
    return message    

def run(process_id: str, each: int, wait: int, extras: dict, dev: bool = False):
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
            start_time = dt.datetime.now()
            message = pipeline(extras)
            execution_time = (dt.datetime.now() - start_time).total_seconds()*1000
            if dev:
                with open('json_data.json', 'w') as out:
                    out.write(json.dumps(message, indent=4))
            
            logger.info(f'From process - {process_id} - It took {execution_time} ms')
        except KeyError as err:
            logger.error(err, exc_info=True)
            break
        except Exception as err:
            logger.warning(f'From process - {process_id} - problem rescuing data. I\'ll try next time')
            logger.warning(err, exc_info=True)
            
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
    configurations: dict = parser.executions()
    all_processes = list()
    dev = parser.dev_environment()
    
    for db, conf in configurations.items():
        each, wait, extras = compute_execution_times(db, conf)
        extras = match_data(db, extras, parser)
        p = Process(target=run, args=(db, each, wait, extras, dev,))
        all_processes.append(p)
        p.start()
    
    # Avoids kill main thred waiting child processors die
    for p in all_processes:
        p.join()