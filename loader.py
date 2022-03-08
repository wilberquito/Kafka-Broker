from configparser import ConfigParser


def db_config(filename='settings.ini', defaults_section='defaults'):
    """Rescues database configuration from settings file"""

    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    defaults = {}

    if parser.has_section(defaults_section):
        params = parser.items(defaults_section)
        for [k, v] in params:
            defaults[k] = v
    else:
        raise Exception(
            f"Default section not in configuration'{filename}' file")

    if ('database' not in defaults):
        raise Exception('No configuration for database found')

    section = defaults['database']

    if parser.has_section(section):
        params = parser.items(section)
        for [k, v] in params:
            db[k] = v
    else:
        raise Exception(
            'Section {0} not found in the {1} file'.format(section, filename))

    return db


print(db_config())
