
# %%
from configparser import ConfigParser
from sqlalchemy import create_engine
import pandas as pd
import json
import time

import constants as const
from yamlparser import yaml_to_dict

# %%


def config(filename):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    defaults = {}

    if parser.has_section(const.DEFAULT_SECTION):
        params = parser.items(const.DEFAULT_SECTION)
        # checking if all default configurations appears in file
        for [k, section] in params:
            defaults[k] = section
            if (not parser.has_section(section)):
                raise Exception(
                    f"Defualt section '{section}' not found in '{filename}' file")

    else:
        raise Exception(
            f"Default section not in configuration'{filename}' file")

    return (parser, defaults)

# %%


def db_config(parser, database_section):
    """Rescues database configuration from settings file"""

    # get section, default to postgresql
    config = {}

    if parser.has_section(database_section):
        params = parser.items(database_section)
        for [k, v] in params:
            config[k] = v
    else:
        raise Exception(
            'Section {0} not found'.format(database_section))

    return config

# %%


def connect_str(params, database_name):
    """ Connect to configured database """

    if (database_name == const.POSTGRE):
        return "postgresql://{username}:{password}@{ipaddress}:{port}/{dbname}".format(**params)

# %%


def is_development_env(parser):
    """Tries to find default configuration

        if it's not present returns true
    """
    if parser.has_section(const.ENVIRONMENT_SECTION):
        params = parser.items(const.ENVIRONMENT_SECTION)
        map = dict(params)
        if const.DEVELOP_CONF in map:
            return map[const.DEVELOP_CONF].lower() in ['true', '1', 't', 'y', 'yes',
                                                       'yeah', 'yup', 'certainly', 'uh-huh']
        else:
            False
    return True

# %%


def sql_query(parser):
    if parser.has_section(const.QUERY_SECTION):
        params = dict(parser.items(const.QUERY_SECTION))
        if const.SQL_CONF in params:
            return params[const.SQL_CONF]
        else:
            raise Exception('sql not defined in query section')
    else:
        raise Exception('query section not defined in settings file')

# %%


parser, defaults = config(const.CONFIG_FILE)
db_name = defaults[const.DATABASE_CONF]

# %%

conn = None

sql = sql_query(parser)

# get database section configuration using db name & file parsed
params = db_config(parser, db_name)
# get a connection using a factory function
db_str = connect_str(params, db_name)
# create connection
cnx = create_engine(db_str)

df = pd.read_sql_query(sql, con=cnx)

# %%
to_send = df.to_json(orient='records')
parsed = json.loads(to_send)
json_string = json.dumps(parsed, indent=4)

if is_development_env(parser):
    with open('json_data.json', 'w') as out:
        out.write(json_string)

# %%
# loading data to datastructure
raw_items = json.loads(json_string)

result = []

for raw_item in raw_items:
    map = dict(raw_item)
    localId = map.pop('gid')
    item = {
        'localId': localId,
        'object': map
    }
    result.append(item)

# %%
for item in result:
    print(item)
    time.sleep(1)

# %%
