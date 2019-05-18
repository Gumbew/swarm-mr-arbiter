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
	return response.json()


def map(json_data_obj):
	mapper = json_data_obj["mapper"]
	key_delimiter = json_data_obj["key_delimiter"]
	field_delimiter = json_data_obj["field_delimiter"]
	destination_file = json_data_obj["destination_file"]
	if 'server_source_file' in json_data_obj:
		server_src = json_data_obj['server_source_file']
	else:
		server_src = None

	json_data = open(os.path.join(os.path.dirname(__file__), '..', 'config', 'json', 'data_nodes.json'))
	data = json.load(json_data)
	json_data.close()
	diction = dict()
	diction["map"] = {
		"mapper": mapper,
		"field_delimiter": field_delimiter,
		"key_delimiter": key_delimiter,
		"destination_file": destination_file,
		"server_src": server_src
	}
	for item in data["data_nodes"]:
		url = 'http://' + item["data_node_address"]
		response = requests.post(url, data=json.dumps(diction))
		response.raise_for_status()
	return response.json()


def reduce(json_data_obj):
	key_delimiter = json_data_obj["key_delimiter"]
	destination_file = json_data_obj["destination_file"]
	reducer = json_data_obj['reducer']
	json_data = open(os.path.join(os.path.dirname(__file__), '..', 'config', 'json', 'data_nodes.json'))
	data = json.load(json_data)
	json_data.close()
	diction = dict()
	if 'server_source_file' in json_data_obj:
		server_src = json_data_obj['server_source_file']
	else:
		server_src = None
	diction["reduce"] = {
		"reducer": reducer,
		"key_delimiter": key_delimiter,
		"destination_file": destination_file,
		"server_src": server_src
	}
	for item in data["data_nodes"]:
		url = 'http://' + item["data_node_address"]

		response = requests.post(url, data=json.dumps(diction))

		response.raise_for_status()
	return response.json()


def clear_data(folder_name):
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
	return response.json()
