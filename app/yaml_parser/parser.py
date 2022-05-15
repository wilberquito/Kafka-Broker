import yaml

class Parser:   
    
    settings = None
        
    def __init__(self, filename):
        self.filename = filename
        self.__parser()
    
    def __parser(self):
        with open(self.filename, 'r') as filename:
            self.settings = yaml.load(filename, Loader=yaml.FullLoader)

    def get_settings(self):
        return self.settings
    
    def get_executions(self):
        return self.get_settings()['executions']
    
    def get_definitions(self):
        executions = self.get_executions()
        definitions = self.get_settings()['definitions']
        return { elem: definitions[elem] for elem in executions if elem in definitions }
                
    
if __name__ == '__main__':
    parser = Parser('settings.yaml')
    print(parser.get_definitions())
    