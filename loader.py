from configparser import ConfigParser


def config(filename='settings.ini', defaults_section='defaults'):
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


def db_config(parser, database_section='database'):
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


parser, defaults = config()
db = db_config(parser, defaults['database'])
print(db)
