import logging
from typing import List
import yaml

from constants import SETTING_API_TK, SETTING_CONSUMER_TK, DATABASE_TK, SETTING_DATABASES_TK, SETTING_DEFAULT_TK, SETTING_DEV_TK, SETTING_FTPS_TK, SETTING_EXECUTIONS_TK, SETTING_PASSWD_TK, SETTING_USER_TK

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
        consumer = self.__defaults().get(SETTING_CONSUMER_TK)
        if consumer is None:
            self.logger.critical('From parser - consumer configuration not defined')
            raise Exception('From parser - consumer configuration not defined')
        return consumer
    
    def api_user(self) -> str:
        consumer_conf =  self.__consumer()
        user = consumer_conf.get(SETTING_USER_TK)
        if user is None:
            self.logger.error('From parser - user configuration not defined')
            raise Exception('From parser - user configuration not defined')
        return user
    
    def api_password(self) -> str:
        consumer_conf =  self.__consumer()
        passwd = consumer_conf.get(SETTING_PASSWD_TK)
        if passwd is None:
            self.logger.error('From parser - passwd configuration not defined')
            raise Exception('From parser - passwd configuration not defined')
        return passwd
    
    def api(self) -> str: 
        ''' returns api skeleton. 
        Skeleton can be defined to be replaced in some
        with connector name, in that case return true in second parameter.
        
        This method raise and exception if api is not found
        '''
        consumer_conf =  self.__consumer()
        api_skeleton = consumer_conf.get(SETTING_API_TK)
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
        default = self.__defaults()
        return False if not SETTING_DEV_TK in default else default[SETTING_DEV_TK]
    
    def _executions_settings(self) -> dict:
        return self.conf.get('executions', dict())
    
    def _databases_executions_settings(self) -> dict:
        return self._executions_settings().get(SETTING_DATABASES_TK, dict())
    
    def _ftps_executions_settings(self) -> dict:
        return self._executions_settings().get(SETTING_FTPS_TK, dict())

    def __defaults(self) -> dict:
        default_conf = self.conf.get(SETTING_DEFAULT_TK)
        if default_conf is None:
            self.logger.error('defualt configuration not found')
            raise Exception('default configuration not found')
        return default_conf    
    
    def __executions(self) -> dict:
        default: dict = self.__defaults()
        if default is None:
            raise Exception(self.__executions.__name__ + ' - default section not found')

        supported_executions = [SETTING_DATABASES_TK, SETTING_FTPS_TK]
        executions_dict: dict = default.get(SETTING_EXECUTIONS_TK, dict())
        
        databases_settings: dict = self._databases_executions_settings()
        excel_settings: dict = self._ftps_executions_settings()
        
        # tuple list => [(DATABASE_TK, settings: dict), ('DATABASE', settings: dict), ('EXcEL', settings: dict)]
        result: list = []
        
        for process_type, processes_names in executions_dict.items():
            if not process_type in supported_executions:
                self.logger.warning(f'Process list definition found - {process_type} - but not supported')
                return
            
            names: list = [] if processes_names is None else processes_names
            inner_result: list = []
            if process_type == SETTING_DATABASES_TK:
                inner_result = [(DATABASE_TK,  { 'process_id': name, 'settings': databases_settings.get(name) }) for name in names if name in databases_settings]
                pass
            else:
                inner_result = [('FTP', {'process_id': name, 'settings': excel_settings.get(name) }) for name in names if name in excel_settings]
            result = result + inner_result
       
        return result



if __name__ == '__main__':
    parser: Parser = Parser('settings.yaml')
    # print(parser.dev_environment())
    print(parser.executions())