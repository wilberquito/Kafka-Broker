import datetime as dt
from multiprocessing import Process
import traceback
import time
import pandas as pd

from sqlalchemy import create_engine

import yaml_parser as yp

# TODO: ADD LOGGING

def consum(url: str, sql: str) -> str:
    engine = create_engine(url)
    df = pd.read_sql_query(sql, con=engine)
    return df.to_json(orient='records')


def run(db: str, conf: dict):
    ''' Entry function to exec when a process start.
    By default, process start inmediatily, but you can define a delay
    '''
    if not 'period' in conf:
        print(f'Execution period not defined for - {db} - defualt execution will be after 15m')
    
    sleep_seconds = conf.get('period', 15) * 60
    
    while True:
        start = dt.datetime.now()
        print(f'About to load data at {start} from - {db} - wish me luck')
        try:
            url, sql = conf['url'], conf['sql']
            start_time = dt.datetime.now()
            _ = consum(url, sql)
            end_time = dt.datetime.now()
            execution_time = (end_time - start_time).total_seconds()*1000
            print(f'Data loaded from - {db} - OK. It took {execution_time} miliseconds')
            # print(response)
        except KeyError:
            traceback.print_exc()
            break
        except Exception:
            print(f'Remote problem connection - {db} - I\'ll try next time')

        # sleeping n minuts
        time.sleep(sleep_seconds)
        
if __name__ == '__main__':
    parser = yp.Parser('settings.yaml')
    configurations: dict = parser.executions()
    all_processes = list()
    
    for db, conf in configurations.items():
        p = Process(target=run, args=(db, conf,))
        all_processes.append(p)
        p.start()
        time.sleep(5)
    
    # Avoids kill main thred waiting child processors die
    for p in all_processes:
        p.join()