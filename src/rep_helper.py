import os
import json
import requests
from .config import get_config


config = get_config()


def fetch_api_rep_data():
	""" Return the Telecom API rep json data. """
	if config['IS_PRODUCTION']:
		return fetch_api_prod_rep_data()
	
	return fetch_api_test_rep_data()


def fetch_api_test_rep_data():
	""" Returns JSON payload containing the test data. """
	file_path = os.path.join(config['ROOT_DIR'], 'test_data.json')
	if os.path.exists(file_path) is False:
		raise FileExistsError(f"The test data file was not found.")
	
	with open(file_path) as fp:
		contents = fp.read()
	
	return json.loads(contents)
	

def fetch_api_prod_rep_data():
	""" Returns JSON payload containing the production data. """
	result = requests.get(config['TELECOM_REP_ENDPOINT'])
	if result.status_code != 200:
		result.raise_for_status()
	
	return result.json()


def save_rep():
	""" Saves the rep data. """
	return ''


def save_rep_skill():
	""" Saves the rep skill. """
	return ''


def save_rep_role():
	""" Saves the rep role. """
	return ''


def save_rep_workgroup():
	""" Saves the rep workgroup. """
	return ''
