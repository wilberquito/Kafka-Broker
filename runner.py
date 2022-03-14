import datetime as dt
from random import randrange
from multiprocessing import Process
import time
import pandas as pd
import logging
from sqlalchemy import create_engine
from constants import EACH_TK, MS_HOUR, WAIT_TK

import yaml_parser as yp

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

def run(id: str, each: int, wait: int, extras: dict):
    ''' executes logic waiting until "wait" seconds 
        and then "each" seconds executes loop logic

        constraints: each, wait >= 0
    '''
    time.sleep(wait)
        
    while True:
        logger.info(f'From process - {id} - about to load data, wish me luck')
        try:
            url, sql = extras['url'], extras['sql']
            start_time = dt.datetime.now()
            _ = consum(url, sql)
            execution_time = (dt.datetime.now() - start_time).total_seconds()*1000
            logger.info(f'From process - {id} - It took {execution_time} ms')
        except KeyError as err:
            logger.error(err, exc_info=True)
            break
        except Exception:
            logger.warning(f'From process - {id} - problem rescuing data. I\'ll try next time')
            logger.warning(err, exc_info=True)
        time.sleep(each)
        
if __name__ == '__main__':
    parser = yp.Parser('settings.yaml')
    configurations: dict = parser.executions()
    all_processes = list()
    
    for db, conf in configurations.items():
        each, wait, extras = compute_execution_times(conf)
        p = Process(target=run, args=(db, each, wait, conf,))
        all_processes.append(p)
        p.start()
    
    # Avoids kill main thred waiting child processors die
    for p in all_processes:
        p.join()