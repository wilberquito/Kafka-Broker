# PROGRAM INNER CONSTANTS

DATABASE_TK = 'DATABASE'

FTP_TK = 'FTP'

APP_LOGGER_NAME = 'APP_LOGGER'

APP_LOGGER_FILE_NAME = 'app'

APP_LOGGERS_DIRECTORY = './logs/'

SETTINGS_FILE_NAME = 'settings.yaml'

PROCESS_ID = 'process_id'

PROCESS_CONTEXT = 'process_context'

PROCESS_TYPE = 'process_type'

PROCESS_WAIT = 'process_wait'

PROCESS_REPEAT = 'process_repeat'



# SETTING FILE IDENT TOKENS

SETTING_REPEAT_TK = 'repeat' # how much it takes from last execution to execute process again

SETTING_WAIT_TK = 'wait' # how much it takes to run the process the first time. Type int

SETTING_CONSUMER_TK = 'consumer' # consumer data information key. Type dict 

SETTING_API_TK = 'api' # api definition. Type string

SETTING_USER_TK = 'user' # user definition. Type string

SETTING_PASSWD_TK = 'passwd' # passwd definition. Type string

SETTING_CONNECTOR_NAME_TK = 'connectorName' # connector name that each process need to connect. 
                                    # this variable is used in `SETTING_API_TK` in case you need to replace values
                                    # Type string
SETTING_DEFAULT_TK = 'default' # default wrapper settings. Type dict

SETTING_DEV_TK = 'dev' # usufull to know if the exuction is in developmenent. Type bool

SETTING_URL_TK = 'url' # url database connection

SETTING_SQL_TK = 'sql' # sql to rescue data from sql

SETTING_LOCAL_ID_TK = 'localId' # json node local id

SETTING_PATH_TK = 'path' # excel path to consum

SETTING_DATABASES_TK = 'databases' # dictionary that holds all databases configuration & element that hold list of databases

SETTING_FTPS_TK = 'ftps' #  dictionary that holds all excels configuration & element that hold list of excels

SETTING_EXECUTIONS_TK = 'executions' # dictionary that holds all executions configurations, and dictionary that hold executions types to run

SETTING_DEFAULTS_TK = 'defaults'
