import json
from pymongo import MongoClient
from bson import Code


# Get config data from the config file
def get_config_data():

    config_location = "config/config.json"
    config_file = open(config_location, "r")
    config_data = config_file.read()
    return config_data


# Get wanted info from config data
def get_info(config_data, config_key):

    data = json.loads(config_data)
    field = data[config_key]

    return field


def get_keys():

    config_data = get_config_data()

    client = MongoClient(get_info(config_data, "CLIENT_URL"))
    db = client[get_info(config_data, "DB_NAME")]
    collection_name = get_info(config_data, "COLLECTION_NAME")

    map = Code("function() { for (var key in this) { emit(key, null); } }")
    reduce = Code("function(key, stuff) { return null; }")
    result = db[collection_name].map_reduce(map, reduce, "Attributes")

    keys = {}
    for key in result.distinct('_id'):
        keys[key] = type(key)

    return keys


def transform_types(keys):

    time_count = 0
    str_count = 0
    bool_count = 0
    other_count = 0

    removed_keys = []
    added_keys = {}

    for key, value in keys.items():

        if key == "time":
            keys[key] = "TIMESTAMP"
            time_count += 1

        elif "-" in key:
            new_key = key.replace('-', "_")
            removed_keys.append(key)
            added_keys[new_key] = "VARCHAR"
            str_count += 1

        elif value is str:
            keys[key] = "VARCHAR"
            str_count += 1

        elif value is bool:
            keys[key] = "bit"
            bool_count += 1

        else:
            other_count += 1

    for added_key, value in added_keys.items():
        keys[added_key] = value

    for remover_key in removed_keys:
        del keys[remover_key]

    print("Str count = ", str_count)
    print("Time count = ", time_count)
    print("Bool count = ", bool_count)
    print("Other count = ", other_count)

    return keys








