# %%
import sys
from yamlparser import yaml_to_dict
from devassert import dev_assert

# %%

try:
    conf = yaml_to_dict('settings.yaml')
except Exception as error:
    print("Fail '{func}'. Error:\n {err}".format(
        func=yaml_to_dict.__name__, err=error))
    sys.exit()

# to know if environment execution is in dev or prod
dev = dev_assert(conf, 'default')
# %%
