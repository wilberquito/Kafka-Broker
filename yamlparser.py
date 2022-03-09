# %%
import yaml

# %%


def yaml_to_dict(filename):
    """ Returns none if file could not be read 

        This function can exec an exeption
    """
    with open(filename, 'r') as file:
        parsed = yaml.load(file, Loader=yaml.FullLoader)
        return parsed
# %%


if __name__ == "__main__":
    params = yaml_to_dict('settings.yaml')

    print(params)

# %%
