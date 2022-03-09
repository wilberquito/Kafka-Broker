from configparser import ConfigParser
import psycopg2

import constants as const


def config(filename, defaults_section):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    defaults = {}

    if parser.has_section(defaults_section):
        params = parser.items(defaults_section)
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


def db_config(parser, database_section):
    """Rescues database configuration from settings file"""

    # get section, default to postgresql
    db = {}

    if parser.has_section(database_section):
        params = parser.items(database_section)
        for [k, v] in params:
            db[k] = v
    else:
        raise Exception(
            'Section {0} not found'.format(database_section))

    return db


def connect(params, database_name):
    """ Connect to configured database """

    if database_name == const.POSTGRE:
        conn = psycopg2.connect(**params)

    return conn


conn = None

try:
    # get file config parse & dict of default configuration
    parser, defaults = config(const.CONFIG_FILE, const.DEFAULT_SECTION)
    # get file name from default database configuration
    db_name = defaults[const.DATABASE_SECTION]
    # get database section configuration using db name & file parsed
    params = db_config(parser, db_name)
    # get a connection using a factory function
    conn = connect(params, db_name)

    cur = conn.cursor()

    db_version = cur.execute('SELECT version()')
    db_version = cur.fetchone()

    # display the PostgreSQL database server version
    print(f'Database connection:\n {db_version}')

    # close the communication with the PostgreSQL
    cur.close()

except (psycopg2.DatabaseError) as error:
    print(f"Problem trying to connect to database - '{db_name}'")
except (Exception) as error:
    print(error)
finally:
    if conn is not None:
        conn.close()
        print('DB closed')
