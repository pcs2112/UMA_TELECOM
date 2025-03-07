import os
import json
import requests
from src.config import get_config
from src.mssql_db import execute_sp, get_sp_first_result_set
from src.utils import format_date

config = get_config()


def _get_custom_attributes(custom_attributes):
    custom_attrs = json.dumps(custom_attributes)
    if custom_attrs == '{}':
        custom_attrs = None
    
    return custom_attrs


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


def start_process():
    """ Calls the stored procedure to start the process. """
    results = execute_sp(
        'UMA_TELECOM.PROCESS_API_DATA',
        {
            'MESSAGE': 'START'
        }
    )
    
    result = get_sp_first_result_set(results)
    if not result:
        return False
    
    return result['return_value']


def stop_process(master_load_id):
    """ Calls the stored procedure to stop the process. """
    results = execute_sp(
        'UMA_TELECOM.PROCESS_API_DATA',
        {
            'MESSAGE': 'FINISHED',
            'LOAD_HIST_PKID_IN': master_load_id
        }
    )
    
    result = get_sp_first_result_set(results)
    if not result:
        return False
    
    return result


def save_rep(rep, master_load_id):
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
            'REP_customAttributes': _get_custom_attributes(rep['customAttributes']),
            'REP_dateAdded': format_date(parse_date(rep['dateAdded'])),
            'LOAD_HISTORY_PKID': master_load_id
        },
        out_arg='rep_id'
    )

    result = get_sp_first_result_set(results)
    if not result:
        return False
    
    return result['rep_id']


def save_rep_skill(rep_id, rep_skill, master_load_id):
    """ Saves the rep skill. """
    execute_sp(
        'UMA_TELECOM.SAVE_D_REP_SKILL',
        {
            'D_REP_ID': rep_id,
            'REP_SKILL_ID': rep_skill['$id'],
            'REP_SKILL_displayName': rep_skill['displayName'],
            'REP_SKILL_proficiency': rep_skill['proficiency'],
            'REP_SKILL_desireToUse': rep_skill['desireToUse'],
            'REP_SKILL_id_ALTERNATE': rep_skill['id'],
            'REP_SKILL_dateAdded': format_date(parse_date(rep_skill['dateAdded'])),
            'LOAD_HISTORY_PKID': master_load_id
        }
    )


def save_rep_role(rep_id, rep_role, master_load_id):
    """ Saves the rep role. """
    execute_sp(
        'UMA_TELECOM.SAVE_D_REP_ROLE',
        {
            'D_REP_ID': rep_id,
            'REP_ROLE_ID': rep_role['$id'],
            'REP_ROLE_roleId': rep_role['roleId'],
            'REP_ROLE_name': rep_role['name'],
            'REP_ROLE_id_ALTERNATE': rep_role['id'],
            'REP_ROLE_dateAdded': format_date(parse_date(rep_role['dateAdded'])),
            'LOAD_HISTORY_PKID': master_load_id
        }
    )


def save_rep_workgroup(rep_id, rep_workgroup, master_load_id):
    """ Saves the rep workgroup. """
    execute_sp(
        'UMA_TELECOM.SAVE_D_REP_WORKGROUP',
        {
            'D_REP_ID': rep_id,
            'REP_WORKGROUP_ID': rep_workgroup['$id'],
            'REP_WORKGROUP_Name': rep_workgroup['name'],
            'REP_WORKGROUP_customAttributes': _get_custom_attributes(rep_workgroup['customAttributes']),
            'REP_WORKGROUP_id_ALTERNATE': rep_workgroup['id'],
            'REP_WORKGROUP_dateAdded': format_date(parse_date(rep_workgroup['dateAdded'])),
            'LOAD_HISTORY_PKID': master_load_id
        }
    )


def parse_date(value):
    if value:
        return value.split('.', 1)[0]
    
    return ''
