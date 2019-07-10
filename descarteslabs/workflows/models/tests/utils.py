import json


def json_normalize(x):
    return json.loads(json.dumps(x))
