from yamlparser import yaml_to_dict


def dev_assert(conf, child=None):
    """ Find dev configuration in map first level or child first level if its defined """
    if (not conf is None):
        dev_arr = ['dev', 'development']
        tree = dict(conf)

        if (not child is None and child in conf):
            tree = dict(conf[child])

        for dev in dev_arr:
            if (dev in tree):
                return tree[dev] or tree[dev] in ['true', '1', 't', 'y', 'yes',
                                                  'yeah', 'yup', 'certainly', 'uh-huh']

    return False


if __name__ == "__main__":
    conf = yaml_to_dict('settings.yaml')
    assert not conf is None
    print(dev_assert(conf, 'default'))
