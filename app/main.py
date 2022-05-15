
from importlib.metadata import metadata
from multiprocessing.dummy import Process

import constants as const
import publisher
from log import setup_custom_logger
from yaml_parser.parser import Parser

logger = setup_custom_logger()


def byebye():
    logger.info('User requested shutdown...')
    
if __name__ == '__main__':
    logger.info('Starting app...')

    parser = Parser(const.SETTINGS_FILE_NAME)
    definitions = parser.get_definitions()
    processes = []
    
    for name, metadata in definitions.items():
        p = Process(target=publisher.publish, args=(name, metadata))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()
        
    byebye()
