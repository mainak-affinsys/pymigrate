import ruamel.yaml as yaml

def parse_yaml_from_path(path: str):
    with open(path) as f:
        res = yaml.safe_load(f)
    return res
