import datetime as dt
from random import randrange
from multiprocessing import Process
import time
import pandas as pd
import logging
from sqlalchemy import create_engine
from constants import API_TK, CONNECTOR_NAME_TK, CONSUMER_TK, EACH_TK, MS_HOUR, PASSWORD_TK, USER_TK, WAIT_TK

from yaml_parser.parser import Parser

logging.basicConfig(filename='runner.log', format='%(asctime)s %(levelname)s [%(filename)s:%(lineno)s]  %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
logger = logging.getLogger('runner.py')
logger.setLevel(logging.DEBUG)


def compute_execution_times(extras: dict) -> tuple[int, int, dict]:
    ''' returns times for each execution & first await & finally the resulting 
        dictionary removing `await` & `start` entries.
        
        Time is expected to be in miliseconds. When one of this entries are not present or its configuration is negative returns a random
        number between a second & and hour in miliseconds.
    '''
    each = randrange(1000, MS_HOUR) if extras.get(EACH_TK) is None or extras.get(EACH_TK) < 0 else extras.get(EACH_TK)
    wait = randrange(1000, MS_HOUR) if extras.get(WAIT_TK) is None or extras.get(WAIT_TK) < 0 else extras.get(WAIT_TK)
    
    if EACH_TK in extras:
        extras.pop(EACH_TK)
    if WAIT_TK in extras:
        extras.pop(WAIT_TK)
        
    return (each, wait, extras)

def consum(url: str, sql: str) -> str:
    engine = create_engine(url)
    df = pd.read_sql_query(sql, con=engine)
    return df.to_json(orient='records')

def charge_and_send(extras: dict):
    api, user, password = extras[API_TK], extras[USER_TK], extras[PASSWORD_TK]
    print(api, user, password)
    pass

def run(id: str, each: int, wait: int, extras: dict):
    ''' executes logic waiting until "wait" seconds 
        and then "each" seconds executes loop logic

        constraints: each, wait >= 0
    '''
    time.sleep(wait)
        
    while True:
        logger.info(f'From process - {id} - about to load data, wish me luck')
        try:
            start_time = dt.datetime.now()
            charge_and_send(extras)
            execution_time = (dt.datetime.now() - start_time).total_seconds()*1000
            logger.info(f'From process - {id} - It took {execution_time} ms')
        except KeyError as err:
            logger.error(err, exc_info=True)
            break
        except Exception:
            logger.warning(f'From process - {id} - problem rescuing data. I\'ll try next time')
            logger.warning(err, exc_info=True)
        time.sleep(each)
        
def add_consumer_data(id: str, extras: dict, parser: Parser) -> dict:
    ''' extras has information of current process '''
    api = parser.api()
    replace =  '{' + CONNECTOR_NAME_TK + '}' in api
    if replace:
        connector_name = extras.get(CONNECTOR_NAME_TK)
        if connector_name is None:
            logger.error(f'api consumer should be replaced by there is no connector name defined in \'{id}\'')
            raise Exception(f'api consumer should be replaced by there is no connector name defined in \'{id}\'')
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
    
    for db, conf in configurations.items():
        each, wait, extras = compute_execution_times(conf)
        extras = add_consumer_data(db, extras, parser)
        p = Process(target=run, args=(db, each, wait, extras,))
        all_processes.append(p)
        p.start()
    
    # Avoids kill main thred waiting child processors die
    for p in all_processes:
        p.join()