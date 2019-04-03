import os
import json
import requests
from .config import get_config


config = get_config()


def fetch_data():
	""" Return the telecom rep json data. """
	if config['IS_PRODUCTION']:
		return fetch_prod_data()
	
	return fetch_test_data()


def fetch_test_data():
	""" Returns JSON payload containing the test data. """
	file_path = os.path.join(config['ROOT_DIR'], 'test_data.json')
	if os.path.exists(file_path) is False:
		raise FileExistsError(f"The test data file was not found.")
	
	with open(file_path) as fp:
		contents = fp.read()
	
	return json.loads(contents)
	

def fetch_prod_data():
	""" Returns JSON payload containing the production data. """
	result = requests.get(config['TELECOM_REP_ENDPOINT'])
	if result.status_code != 200:
		result.raise_for_status()
	
	return result.json()
