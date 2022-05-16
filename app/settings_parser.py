import yaml
import enum


APP_LOGGER_NAME = 'APP_LOGGER'

APP_LOGGER_FILE_NAME = 'app'

APP_LOGGERS_DIRECTORY = './logs/'

SETTINGS_FILE_NAME = 'settings.yaml'

# setting settings names

ACTIVE_EXECUTIONS = 'active_executions'

DEFINITIONS = 'definitions'

PROCESS_TYPE = 'type'

REPEAT_EACH_SECONDS = 'repeat_each_seconds'

KAFKA_TOPIC = 'topic'

KAFKA_BOOTSTRAP_SERVER = 'bootstrap_server'

DATABASE_URL = 'url'

DATABASE_SQL = 'sql'

FTP_EXCEL_FILE_NAME = 'file'

FTP_USERNAME = 'user'

FTP_PASSWORD = 'password'

FTP_HOST = 'host'

FTP_PORT = 'port'

class Connection(enum.Enum):
    FTP = 'ftp'
    DATABASE = 'database'

class SettingsParser:   
    
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
    parser = SettingsParser('settings.yaml')
    print(parser.get_definitions())
    