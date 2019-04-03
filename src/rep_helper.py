import os
import json
import requests
from .config import get_config
from src.mssql_connection import execute_sp, get_sp_result_set


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


def save_rep(rep):
	""" Saves the rep data. """
	results = execute_sp(
		'UMA_TELECOM.SAVE_D_REP',
		{
			'REP_ID': rep['$id'],
			'REP_userId': rep['userId'],
			'REP_homeSite': rep['homeSite'],
			'REP_firstName': rep['firstName'],
			'REP_lastName': rep['lastName'],
			'REP_displayName': rep['displayName'],
			'REP_ntDomainUser': rep['ntDomainUser'],
			'REP_extension': rep['extension'],
			'REP_outboundANI': rep['outboundANI'],
			'REP_id_LIST': rep['id'],
			'REP_customAttributes': json.dumps(rep['customAttributes']),
			'REP_dateAdded': rep['dateAdded']
		}
	)
	
	return get_sp_result_set(results)


def save_rep_skill(rep_id, rep_skill):
	""" Saves the rep skill. """
	results = execute_sp(
		'UMA_TELECOM.SAVE_D_REP_SKILL',
		{
			'REP_SKILL_ID': rep_skill['$id'],
			'REP_SKILL_displayName': rep_skill['displayName'],
			'REP_SKILL_proficiency': rep_skill['proficiency'],
			'REP_SKILL_desireToUse': rep_skill['desireToUse'],
			'REP_SKILL_id_ALTERNATE': rep_skill['id'],
			'REP_SKILL_dateAdded': rep_skill['dateAdded']
		}
	)
	
	return get_sp_result_set(results)


def save_rep_role(rep_id, rep_role):
	""" Saves the rep role. """
	results = execute_sp(
		'UMA_TELECOM.SAVE_D_REP_ROLE',
		{
			'REP_ROLE_ID': rep_role['$id'],
			'REP_ROLE_roleId': rep_role['roleId'],
			'REP_ROLE_name': rep_role['name'],
			'REP_ROLE_id_ALTERNATE': rep_role['id'],
			'REP_SKILL_dateAdded': rep_role['dateAdded']
		}
	)
	
	return get_sp_result_set(results)


def save_rep_workgroup(rep_id, rep_workgroup):
	""" Saves the rep workgroup. """
	results = execute_sp(
		'UMA_TELECOM.SAVE_D_REP_WORKGROUP',
		{
			'REP_WORKGROUP_ID': rep_workgroup['$id'],
			'REP_WORKGROUP_Name': rep_workgroup['name'],
			'REP_WORKGROUP_customAttributes': json.dumps(rep_workgroup['customAttributes']),
			'REP_WORKGROUP_id_ALTERNATE': rep_workgroup['id'],
			'REP_WORKGROUP_dateAdded': rep_workgroup['dateAdded']
		}
	)
	
	return get_sp_result_set(results)
