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
    
    def _executions_settings(self) -> dict:
        return self.conf.get('executions', dict())
    
    def _databases_executions_settings(self) -> dict:
        return self._executions_settings().get('databases', dict())
    
    def _excel_executions_settings(self) -> dict:
        return self._executions_settings().get('excels', dict())

    def __defaults(self) -> dict:
        default_conf = self.conf.get(DEFAULT_TK)
        if default_conf is None:
            self.logger.error('defualt configuration not found')
            raise Exception('default configuration not found')
        return default_conf    
    
    def __executions(self) -> dict:
        default: dict = self.__defaults()
        if default is None:
            raise Exception(self.__executions.__name__ + ' - default section not found')

        supported_executions = ['databases', 'excels']
        executions_dict: dict = default.get('executions', dict())
        
        databases_settings: dict = self._databases_executions_settings()
        excel_settings: dict = self._excel_executions_settings()
        
        # tuple list => [('DATABASE', settings: dict), ('DATABASE', settings: dict), ('EXcEL', settings: dict)]
        result: list = []
        
        for process_type, processes_names in executions_dict.items():
            if not process_type in supported_executions:
                self.logger.warning(f'Process list definition found - {process_type} - but not supported')
                return
            
            names: list = [] if processes_names is None else processes_names
            inner_result: list = []
            if process_type == 'databases':
                inner_result = [('DATABASE',  { 'process_id': name, 'settings': databases_settings.get(name) }) for name in names if name in databases_settings]
                pass
            else:
                inner_result = [('EXCEL', {'process_id': name, 'settings': excel_settings.get(name) }) for name in names if name in excel_settings]
            result = result + inner_result
       
        return result



if __name__ == '__main__':
    parser: Parser = Parser('settings.yaml')
    # print(parser.dev_environment())
    print(parser.executions())