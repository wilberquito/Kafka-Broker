import datetime as dt
import json
from multiprocessing import Process
import time
import requests
from requests import exceptions as rex
import pandas as pd
from sqlalchemy import create_engine
import ftp_retriever as ftpret

import constants as const
from loggerapp import logger_app

logger = logger_app()

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

def _charge_from_ftp(user: str, passwd: str, host: str, filename: str, port:int = 21):
    arr_bytes: bytes = ftpret.retrieve_bytes(filename, host, passwd, user, port)
    df = pd.read_excel(arr_bytes)
    return df.to_json(orient='records')
    
def ftp_pipeline(user: str, passwd: str, host: str, filename: str, localId: str, consumer: dict, port: int = 21):
    json = _charge_from_ftp(user, passwd, host, filename, port)
    map_and_send_to_consumer(json, localId, consumer)
        
def database_pipeline(url, sql, localId, consumer) -> list:
    """ Consum data -> Map data -> Send data """
    json = charge_from_database(url, sql)
    map_and_send_to_consumer(json, localId, consumer)    

def _run(process_type: str, process_id: str, repeat: int, wait: int, context: dict):
    """ Handles execution time and execution itself
    
        When one key is not found inside context, process ends because a bad configuration
        or bad read parser configuration.
        
        Constraints: repeat, wait >= 0
    """
    if wait > 0:
        logger.info(f'Process - {process_id} - will make it first execution in {wait} seconds')
        time.sleep(wait)
        
    while True:
        logger.info(f'Process - {process_id} - about to load data, wish me luck')
        try:
            start_time = dt.datetime.now()
            match process_type:
                case 'DATABASE':
                    database_pipeline(**context)
                case 'FTP':
                    ftp_pipeline(**context)
            execution_time = (dt.datetime.now() - start_time).total_seconds()*1000
            logger.info(f'Process - {process_id} - It took {execution_time} ms')
        except rex.ConnectionError:
            logger.warning(f'Process - {process_id} - problem sending data. I\'ll try next time')
        except Exception:
            logger.error(f'Process - {process_id} - unhandled exception')
        time.sleep(repeat if repeat >= 0 else 0)
        

def run(executions: list):
    processes = []
    for settings in executions:
        process_type = settings[const.PROCESS_TYPE]
        process_id = settings[const.PROCESS_ID]
        repeat = settings[const.PROCESS_REPEAT]
        wait = settings[const.PROCESS_WAIT]
        context = settings[const.PROCESS_CONTEXT]
        p = Process(target=_run, args=(process_type, process_id, repeat, wait, context,))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()