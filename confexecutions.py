

from yamlparser import yaml_to_dict


def conf_executions(conf):
    """ Returns a list of executions defined in default entry in map.
        Ignores those that does not appears in conf map
    """

    executions = conf['default']['executions']
    executions = [] if executions is None else executions

    executions_values = conf['executions']
    if (executions_values is None):
        return []
    
    return list(filter(None, [executions_values[execution] for execution in executions]))


if __name__ == '__main__':
    conf = yaml_to_dict('settings.yaml')
    ls = conf_executions(conf)

    print(ls)
