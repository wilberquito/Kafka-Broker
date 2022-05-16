
from importlib.metadata import metadata
from multiprocessing.dummy import Process

import publisher
from log import setup_custom_logger
from settings_parser import SettingsParser

import settings_parser as parser

logger = setup_custom_logger()
    
if __name__ == '__main__':
    logger.info('Starting app...')

    parser = SettingsParser(parser.SETTINGS_FILE_NAME)
    running_executions = parser.get_running_executions()
    processes = []
    
    for name, metadata in running_executions.items():
        p = Process(target=publisher.publish, args=(name, metadata))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()
        
    logger.info('Broker loader shuts down...')

