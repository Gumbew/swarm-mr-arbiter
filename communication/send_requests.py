# Send tasks to datanodes like Map(), Shuffle(), Reduce() etc.
# Send request to mkfile to datanodes
import json
import os.path
import requests


def make_file(file_name):
    json_data = open(os.path.dirname(__file__) + "\\..\\config\\json\\data_nodes.json")
    data = json.load(json_data)
    json_data.close()
    diction = dict()
    diction["make_file"] ={
        "file_name": file_name
    }
    for item in data["data_nodes"]:
        url = 'http://' + item["data_node_address"]

        response = requests.post(url, data=json.dumps(diction))

        response.raise_for_status()
        print(response.json())
    return response.json()
