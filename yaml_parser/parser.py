from typing import Dict, List
import yaml

class Parser:

    conf = None
    file = None
        
    def __init__(self, file) -> None:
        self.file = file
        self.__parse()
        
    def dev_environment(self) -> bool:
        return self.__dev_environment()
    
    def executions(self) -> List:
        return self.__executions()
    
    def __parse(self) -> None:
        with open(self.file, 'r') as file:
            parsed = yaml.load(file, Loader=yaml.FullLoader)
            self.conf = parsed
        
    def __dev_environment(self) -> bool:
        ''' Checks in conf if it was configured as dev environment '''

        dev_ident = 'dev'
        default = self.__defaults()

        if default is None:
            raise Exception(self.__dev_environment + '- default section not found')

        return False if not dev_ident in default else default[dev_ident]

    def __defaults(self) -> Dict:
        default_ident = 'default'
        return self.conf[default_ident] if default_ident in self.conf else None 
    
    def __executions(self) -> Dict:
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