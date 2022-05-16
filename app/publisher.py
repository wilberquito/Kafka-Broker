
from random import random
import time
import pandas as pd
import json
from kafka import KafkaProducer
from ftp_retriever import retrieve_bytes

from sqlalchemy import create_engine
from log import setup_custom_logger

import settings_parser as parser

logger = setup_custom_logger()

def publish(id, context):
    process_type = context['type']
    repeat_each_seconds = context.get('repeat_each_seconds', (random() + 1) * 600)

    keepOn = True    

    while keepOn:
        
        logger.info(f'Process - {id} - about to load data...')
        
        match process_type:
            case parser.Connection.DATABASE.value:
                database_pipeline(**context)
            case parser.Connection.FTP.value:
                ftp_pipeline(**context)
            case _:
                print(f'None supported source type - {process_type}')
                keepOn = False
        
        if keepOn:
            time.sleep(repeat_each_seconds)

def database_pipeline(**context):
    sql = context[parser.DATABASE_SQL]
    url = context[parser.DATABASE_URL]
    
    captured = charge_from_database(url, sql)
    send(captured, **context)

def charge_from_database(url: str, sql: str) -> str:
    """ Consumes database and maps to json """
    engine = create_engine(url)
    con = engine.connect(close_with_result=True)
    df = pd.read_sql_query(sql, con=con)
    return df.to_json(orient='records')

def ftp_pipeline(**context):
    file_name = context[parser.FTP_EXCEL_FILE_NAME]
    user = context[parser.FTP_USERNAME]
    password = context[parser.FTP_PASSWORD]
    host = context[parser.FTP_HOST]
    port = context.get(parser.FTP_PORT, 21)
    
    captured = charge_from_ftp(file_name, host, password, user, port)
    send(captured, **context)

def charge_from_ftp(file_name, host, password, user, port):
    btes = retrieve_bytes(file_name, host, password, user, port)
    df = pd.read_excel(btes)
    return df.to_json(orient='records')
    
def send(message: str, **context):
    topic = context[parser.KAFKA_TOPIC]
    bootstrap_server = context[parser.KAFKA_BOOTSTRAP_SERVER]

    kafka_producer = KafkaProducer(bootstrap_servers=bootstrap_server)
    
    loads = json.loads(message)

    for commit in loads:
        bytes_commit = bytes(json.dumps(commit), 'utf-8')
        future = kafka_producer.send(topic, bytes_commit)
        _ = future.get(timeout=60)
        str_commit = bytes_commit.decode('utf-8')
        logger.info(f"Json Object send - {str_commit}")
