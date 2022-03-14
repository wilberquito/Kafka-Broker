import logging
from typing import Dict, List
import yaml

from constants import API_TK, CONSUMER_TK, DEFAULT_TK, PASSWORD_TK, USER_TK

class Parser:
    logging.basicConfig(filename='parser.log', format='%(asctime)s %(levelname)s [%(filename)s:%(lineno)s]  %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    logger = logging.getLogger('parser.py')
    logger.setLevel(logging.DEBUG)

    conf = None
    file = None
        
    def __init__(self, file) -> None:
        self.file = file
        self.__parse()
        
    def dev_environment(self) -> bool:
        return self.__dev_environment()
    
    def executions(self) -> List:
        return self.__executions()
    
    def __consumer(self) -> dict:
        ''' returns consumer configuration '''
        consumer = self.__defaults().get(CONSUMER_TK)
        if consumer is None:
            self.logger.critical('From parser - consumer configuration not defined')
            raise Exception('From parser - consumer configuration not defined')
        return consumer
    
    def api_user(self) -> str:
        consumer_conf =  self.__consumer()
        user = consumer_conf.get(USER_TK)
        if user is None:
            self.logger.error('From parser - user configuration not defined')
            raise Exception('From parser - user configuration not defined')
        return user
    
    def api_password(self) -> str:
        consumer_conf =  self.__consumer()
        password = consumer_conf.get(PASSWORD_TK)
        if password is None:
            self.logger.error('From parser - password configuration not defined')
            raise Exception('From parser - password configuration not defined')
        return password
    
    def api(self) -> str: 
        ''' returns api skeleton. 
        Skeleton can be defined to be replaced in some
        with connector name, in that case return true in second parameter.
        
        This method raise and exception if api is not found
        '''
        consumer_conf =  self.__consumer()
        api_skeleton = consumer_conf.get(API_TK)
        if api_skeleton is None:
            self.logger.error('From parser - api_skeleton configuration not defined')
            raise Exception('From parser - api_skeleton configuration not defined')
        return api_skeleton
                
    def __parse(self) -> None:
        with open(self.file, 'r') as file:
            parsed = yaml.load(file, Loader=yaml.FullLoader)
            self.conf = parsed
        
    def __dev_environment(self) -> bool:
        ''' Checks in conf if it was configured as dev environment '''
        dev_ident = 'dev'
        default = self.__defaults()
        return False if not dev_ident in default else default[dev_ident]

    def __defaults(self) -> dict:
        default_conf = self.conf.get(DEFAULT_TK)
        if default_conf is None:
            self.logger.error('defualt configuration not found')
            raise Exception('default configuration not found')
        return default_conf    
    
    def __executions(self) -> dict:
        default = self.__defaults()
        if default is None:
            raise Exception(self.__executions.__name__ + ' - default section not found')
 
        executions_names = default.get('executions', list())
        executions = self.conf.get('executions', dict())
        result = dict()
        for name in executions_names:
            if not executions.get(name) is None:
                result[name] = executions.get(name)
        return result


if __name__ == '__main__':
    parser: Parser = Parser('settings.yaml')
    print(parser.dev_environment())
    print(parser.executions())