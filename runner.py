import datetime as dt
from multiprocessing import Pool, Process
import random as rand
import yaml_parser as yp
import time

def run(db, conf, delay=0):
    ''' Entry function to exec when a process start.
    By default, process start inmediatily, but you can define a delay
    '''
    time.sleep(delay)
    start = dt.datetime.now()
    print(f"{db} starts at {start}")
    
    while True:
        period = conf.get('period', 10)
        time.sleep(period)
        
if __name__ == '__main__':
    parser = yp.Parser('settings.yaml')
    configurations: dict = parser.executions()
    random_delay: int = parser.random_delay()
    all_processes = list()
    
    for db, conf in configurations.items():
        p = Process(target=run, args=(db, conf, rand.randrange(random_delay),))
        all_processes.append(p)
        p.start()
    
    # waiting to all processos end
    for p in all_processes:
        p.join()