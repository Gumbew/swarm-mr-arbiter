# Send tasks to datanodes like Map(), Shuffle(), Reduce() etc.
# Send request to mkfile to datanodes
import json
import os.path
import requests


def make_file(file_name):
    json_data = open(os.path.join(os.path.dirname(__file__), '..', 'config', 'json', 'data_nodes.json'))
    data = json.load(json_data)
    json_data.close()
    diction = dict()
    diction["make_file"] = {
        "file_name": file_name
    }
    for item in data["data_nodes"]:
        url = 'http://' + item["data_node_address"]

        response = requests.post(url, data=json.dumps(diction))

        response.raise_for_status()
        print(response.json())
    return response.json()


def map(mapper, field_delimiter, key_delimiter, destination_file):
    json_data = open(os.path.join(os.path.dirname(__file__), '..', 'config', 'json', 'data_nodes.json'))
    data = json.load(json_data)
    json_data.close()
    diction = dict()
    diction["map"] = {
        "mapper": mapper,
        "field_delimiter": field_delimiter,
        "key_delimiter": key_delimiter,
        "destination_file": destination_file
    }
    for item in data["data_nodes"]:
        url = 'http://' + item["data_node_address"]

        response = requests.post(url, data=json.dumps(diction))

        response.raise_for_status()
        print(response.json())
    return response.json()


def reduce(reducer, key_delimiter, destination_file):
    json_data = open(os.path.join(os.path.dirname(__file__), '..', 'config', 'json', 'data_nodes.json'))
    data = json.load(json_data)
    json_data.close()
    diction = dict()
    diction["reduce"] = {
        "reducer": reducer,
        "key_delimiter": key_delimiter,
        "destination_file": destination_file
    }
    for item in data["data_nodes"]:
        url = 'http://' + item["data_node_address"]

        response = requests.post(url, data=json.dumps(diction))

        response.raise_for_status()
        print(response.json())
    return response.json()


def clear_data(folder_name):
    print('CLEAR_DATA_STARTED')
    diction = dict()
    diction['clear_data'] = {
        'folder_name': folder_name
    }
    json_data = open(os.path.join(os.path.dirname(__file__), '..', 'config', 'json', 'data_nodes.json'))
    data = json.load(json_data)
    json_data.close()
    for item in data['data_nodes']:
        url = 'http://' + item['data_node_address']
        response = requests.post(url, data=json.dumps(diction))
        response.raise_for_status()
    print('CLEAR_DATA_FINISHED')
    return response.json()
