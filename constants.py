# SETTING FILE IDENT TOKENS
REPEAT_TK = 'repeat' # how much it takes from last execution to execute process again

WAIT_TK = 'wait' # how much it takes to run the process the first time. Type int

CONSUMER_TK = 'consumer' # consumer data information key. Type dict 

API_TK = 'api' # api definition. Type string

USER_TK = 'user' # user definition. Type string

PASSWORD_TK = 'password' # password definition. Type string

CONNECTOR_NAME_TK = 'connectorName' # connector name that each process need to connect. 
                                    # this variable is used in `API_TK` in case you need to replace values
                                    # Type string
DEFAULT_TK = 'default' # default wrapper settings. Type dict

DEV_TK = 'dev' # usufull to know if the exuction is in developmenent. Type bool

URL_TK = 'url' # url database connection

SQL_TK = 'sql' # sql to rescue data from sql

LOCAL_ID_TK = 'localId' # json node local id

PATH_TK = 'path' # excel path to consum
