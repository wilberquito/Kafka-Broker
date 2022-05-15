
from random import random
import time
import pandas as pd
import json
from kafka import KafkaProducer


from sqlalchemy import create_engine
from log import setup_custom_logger

logger = setup_custom_logger()

def charge_from_database(url: str, sql: str) -> str:
    """ Consumes database and maps to json """
    engine = create_engine(url)
    con = engine.connect(close_with_result=True)
    df = pd.read_sql_query(sql, con=con)
    return df.to_json(orient='records')

def send(message: str, **context):
    topic = context['topic']
    bootstrap_server = context['bootstrap_server']

    kafka_producer = KafkaProducer(bootstrap_servers=bootstrap_server)
    
    loads = json.loads(message)

    for commit in loads:
        bytes_commit = bytes(json.dumps(commit), 'utf-8')
        future = kafka_producer.send(topic, bytes_commit)
        _ = future.get(timeout=60)

def database_pipeline(**context):
    sql = context['sql']
    url = context['url']
    
    captured = charge_from_database(url, sql)
    send(captured, **context)

def publish(id, context):
    ty = context['type']
    repeat_each_seconds = context.get('repeat_each_seconds', (random() + 1) * 600)

    keepOn = True    

    while keepOn:
        match ty:
            case 'database':
                database_pipeline(**context)
            case _:
                print(f'None supported source type - {ty}')
                keepOn = False
        
        if keepOn:
            time.sleep(repeat_each_seconds)